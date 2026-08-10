//! Rendering. The live view shows how many keys each consumer currently has wrong;
//! the final report shows which keys are wrong *after everything has settled*,
//! which is the number that matters.

use crate::scenario::{LiveState, Outcome, Phase, RunConfig, key_name, run_once};
use anyhow::Result;
use crossterm::event::{self, Event as TermEvent, KeyCode};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use std::io::stdout;
use std::sync::atomic::Ordering;
use std::time::Duration;

pub async fn run_with_tui(config: RunConfig) -> Result<Outcome> {
    enable_raw_mode()?;
    let mut out = stdout();
    crossterm::execute!(out, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(out))?;

    let result = run_once(config, |state| {
        terminal.draw(|frame| draw(frame, state))?;
        if event::poll(Duration::from_millis(0))?
            && let TermEvent::Key(key) = event::read()?
            && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            return Ok(false);
        }
        Ok(true)
    })
    .await;

    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn draw(frame: &mut Frame, state: &LiveState) {
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(6),
        Constraint::Length(4),
    ])
    .split(frame.area());

    let phase = state.phase();
    let phase_style = match phase {
        Phase::Stalled => Style::default().fg(Color::Yellow).bold(),
        Phase::Quiesced => Style::default().fg(Color::Magenta).bold(),
        _ => Style::default().fg(Color::Green),
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                format!(" {} ", state.mode.short()),
                Style::default().fg(Color::Black).bg(Color::Cyan).bold(),
            ),
            Span::raw("  "),
            Span::styled(phase.label(&state.victim), phase_style),
        ]))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Felix — local state divergence "),
        ),
        chunks[0],
    );

    let wrong = state
        .wrong_now
        .try_lock()
        .map(|w| w.clone())
        .unwrap_or_default();
    let rows: Vec<Row> = state
        .consumers
        .iter()
        .enumerate()
        .map(|(idx, consumer)| {
            let stalled = consumer.stalled.load(Ordering::Relaxed);
            let wrong_count = wrong.get(idx).copied().unwrap_or(0);
            let style = if wrong_count > 0 {
                Style::default().fg(Color::Red).bold()
            } else if stalled {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::Green)
            };
            Row::new(vec![
                Cell::from(consumer.name.clone()),
                Cell::from(if stalled { "STALLED" } else { "applying" }),
                Cell::from(format!("{}", consumer.applied.load(Ordering::Relaxed))),
                Cell::from(format!("{wrong_count} / {}", state.keys)),
            ])
            .style(style)
        })
        .collect();

    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(14),
                Constraint::Length(10),
                Constraint::Length(14),
                Constraint::Length(16),
            ],
        )
        .header(
            Row::new(vec!["consumer", "state", "applied", "keys WRONG"])
                .style(Style::default().fg(Color::DarkGray)),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" local copies vs the authority "),
        ),
        chunks[1],
    );

    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::raw(format!(
                "  authority: {} changes published across {} keys",
                state.published.load(Ordering::Relaxed),
                state.keys
            ))),
            Line::from(Span::styled(
                "  a consumer is never told it is wrong    [q] quit",
                Style::default().fg(Color::DarkGray),
            )),
        ])
        .block(Block::default().borders(Borders::ALL)),
        chunks[2],
    );
}

pub async fn run_with_plain(config: RunConfig) -> Result<Outcome> {
    let mode = config.mode;
    println!("\n=== {} ===", mode.label());
    let mut last: Option<Phase> = None;
    run_once(config, move |state| {
        let phase = state.phase();
        if last != Some(phase) {
            println!("\n-- {} --", phase.label(&state.victim));
            last = Some(phase);
        }
        let wrong = state
            .wrong_now
            .try_lock()
            .map(|w| w.clone())
            .unwrap_or_default();
        let summary: Vec<String> = state
            .consumers
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                format!(
                    "{}{}={} wrong",
                    c.name,
                    if c.stalled.load(Ordering::Relaxed) {
                        "(STALLED)"
                    } else {
                        ""
                    },
                    wrong.get(idx).copied().unwrap_or(0)
                )
            })
            .collect();
        println!(
            "  {}  | published={}",
            summary.join("  "),
            state.published.load(Ordering::Relaxed)
        );
        Ok(true)
    })
    .await
}

/// The report. Everything has stopped and settled, so anything still wrong is
/// permanently wrong.
pub fn print_report(outcomes: &[Outcome]) {
    println!("\n{}", "=".repeat(74));
    println!("  Local state after everything settled");
    println!("{}", "=".repeat(74));

    for outcome in outcomes {
        println!("\n  {}", outcome.mode.label());
        println!("    {} changes published\n", outcome.published);
        for (name, applied, wrong, stalled) in &outcome.consumers {
            let marker = if *stalled { "  <- stalled mid-run" } else { "" };
            if wrong.is_empty() {
                println!("    {name:<13} applied {applied:>8}   state CORRECT{marker}");
            } else {
                println!(
                    "    {name:<13} applied {applied:>8}   {} of {} keys PERMANENTLY WRONG{marker}",
                    wrong.len(),
                    outcome.keys
                );
                // A few by name; the full list is noise once it runs to hundreds.
                for key in wrong.iter().take(6) {
                    println!("                    - {}", key_name(*key));
                }
                if wrong.len() > 6 {
                    println!("                    - ... and {} more", wrong.len() - 6);
                }
            }
        }
    }

    if outcomes.len() == 2 {
        let (lossy, lossless) = (&outcomes[0], &outcomes[1]);
        println!("\n{}", "-".repeat(74));
        println!(
            "  wrong keys after settling:  {} under {}   vs  {} under {}",
            lossy.total_wrong(),
            lossy.mode.short(),
            lossless.total_wrong(),
            lossless.mode.short()
        );
        println!("{}", "-".repeat(74));
    }

    println!(
        "
  What this shows

  A consumer holding a local copy of state is not merely behind when an update is
  dropped — it is wrong, and it stays wrong. There is no redelivery, no gap signal,
  and nothing in the API that would let it discover the problem on its own. The
  keys listed above are ones whose most recent change never arrived; they will stay
  incorrect until something happens to change them again, which for a cold key may
  be never.

  Hot keys largely self-heal, because the next change to the same key overwrites
  the mistake. That is why the damage concentrates in rarely-changed keys, and why
  a uniform workload would make this look far less serious than it is.

  This is the gap between what Felix does today and what it is aimed at. From
  docs-site/docs/getting-started/what-felix-is-for.md:

    \"For an event feed, a dropped message means a consumer missed one update. For
     a consumer maintaining a local copy of state, a dropped message means its
     local copy is permanently wrong with no signal that would let it recover on
     its own.\"

  Closing it needs gap-free snapshot-plus-stream subscribe, resumable
  subscriptions, or an explicit resynchronisation protocol. Until one of those
  exists, live-state synchronisation is a direction, not a supported use case.

  The lossless column is not the answer either: it converges only by letting the
  slowest consumer throttle the publisher, which is the trade-off the
  slow-consumer-isolation demo measures.

  Single-node, loopback, {} keys, {} consumers.
",
        outcomes.first().map(|o| o.keys).unwrap_or(0),
        outcomes.first().map(|o| o.consumers.len()).unwrap_or(0)
    );
}
