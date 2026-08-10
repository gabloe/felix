//! Rendering. Two backends over the same scenario: a ratatui terminal UI, and a
//! plain-text fallback used by `--no-tui`, by the inline test, and by anything
//! running without a TTY.

use crate::scenario::{LiveState, Phase, RunConfig, RunOutcome, run_once};
use anyhow::Result;
use crossterm::event::{self, Event as TermEvent, KeyCode};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table};
use std::io::stdout;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Run a scenario with the terminal UI.
pub async fn run_with_tui(config: RunConfig) -> Result<RunOutcome> {
    enable_raw_mode()?;
    let mut out = stdout();
    crossterm::execute!(out, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(out);
    let mut terminal = Terminal::new(backend)?;

    // Restore the terminal even if the scenario returns an error, otherwise a
    // failure leaves the user's shell in raw mode with no echo.
    let result = run_once(config, |state| {
        terminal.draw(|frame| draw(frame, state))?;
        // Non-blocking key check so the demo stays interruptible.
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
        Constraint::Min(7),
        Constraint::Length(5),
    ])
    .split(frame.area());

    let phase = state.phase();
    let phase_style = match phase {
        Phase::Degraded => Style::default().fg(Color::Yellow).bold(),
        _ => Style::default().fg(Color::Green),
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            format!(" policy = {} ", state.policy.label()),
            Style::default().fg(Color::Black).bg(Color::Cyan).bold(),
        ),
        Span::raw("  "),
        Span::styled(phase.label(&state.victim), phase_style),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Felix — slow-consumer isolation "),
    );
    frame.render_widget(header, chunks[0]);

    let rows: Vec<Row> = state
        .subscribers
        .iter()
        .map(|stat| {
            let stalled = stat.stalled.load(Ordering::Relaxed);
            let received = stat.received.load(Ordering::Relaxed);
            let gaps = stat.gaps.load(Ordering::Relaxed);
            let style = if stalled {
                Style::default().fg(Color::Red).bold()
            } else if gaps > 0 {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::Green)
            };
            Row::new(vec![
                Cell::from(stat.name.clone()),
                Cell::from(if stalled { "STALLED" } else { "draining" }),
                Cell::from(format!("{received}")),
                Cell::from(format!("{gaps}")),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(14),
        ],
    )
    .header(
        Row::new(vec!["consumer", "state", "received", "lost"])
            .style(Style::default().fg(Color::DarkGray)),
    )
    .block(Block::default().borders(Borders::ALL).title(" consumers "));
    frame.render_widget(table, chunks[1]);

    let published = state.publisher.published.load(Ordering::Relaxed);
    let ratio = if state.target_rate > 0 {
        // Show the publisher's health as progress toward its target rate. Under
        // `block` this is the bar that visibly collapses.
        (published as f64 / state.target_rate.max(1) as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let footer = Layout::vertical([Constraint::Length(3), Constraint::Length(2)]).split(chunks[2]);
    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title(format!(
            " publisher — {published} published, target {}/s ",
            state.target_rate
        )))
        .gauge_style(Style::default().fg(if ratio > 0.5 {
            Color::Green
        } else {
            Color::Red
        }))
        .ratio(ratio);
    frame.render_widget(gauge, footer[0]);

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!("  {}    [q] quit", state.policy.tradeoff()),
            Style::default().fg(Color::DarkGray),
        ))),
        footer[1],
    );
}

/// Run a scenario with periodic plain-text output.
pub async fn run_with_plain(config: RunConfig) -> Result<RunOutcome> {
    let policy = config.policy;
    println!(
        "\n=== policy = {}  ({}) ===",
        policy.label(),
        policy.tradeoff()
    );
    let mut last_phase: Option<Phase> = None;
    run_once(config, move |state| {
        let phase = state.phase();
        if last_phase != Some(phase) {
            println!("\n-- {} --", phase.label(&state.victim));
            last_phase = Some(phase);
        }
        let mut line = String::new();
        for stat in &state.subscribers {
            let stalled = stat.stalled.load(Ordering::Relaxed);
            line.push_str(&format!(
                "{}{}={}/{} lost  ",
                stat.name,
                if stalled { "(STALLED)" } else { "" },
                stat.received.load(Ordering::Relaxed),
                stat.gaps.load(Ordering::Relaxed),
            ));
        }
        println!(
            "  {line}| published={}",
            state.publisher.published.load(Ordering::Relaxed)
        );
        Ok(true)
    })
    .await
}

/// The payoff: both policies, side by side.
pub fn print_comparison(outcomes: &[RunOutcome]) {
    println!("\n{}", "=".repeat(72));
    println!("  Slow-consumer isolation — one consumer stalled, same workload");
    println!("{}", "=".repeat(72));

    for outcome in outcomes {
        println!("\n  policy = {}", outcome.policy.label());
        println!(
            "    publisher      {:>10} msg/s achieved   (target {}/s)",
            outcome.achieved_rate, outcome.target_rate
        );
        for (name, received, gaps, stalled) in &outcome.subscribers {
            println!(
                "    {:<14} {:>10} received  {:>10} lost{}",
                name,
                received,
                gaps,
                if *stalled { "   <- stalled" } else { "" }
            );
        }
        if let Some(p99) = outcome.healthy_p99_us_degraded {
            println!(
                "    healthy p99    {:>10} us  while the other consumer was stalled",
                p99
            );
        }
    }

    if outcomes.len() == 2 {
        let (a, b) = (&outcomes[0], &outcomes[1]);
        println!("\n{}", "-".repeat(72));
        println!("  What changed between the two runs:");
        println!(
            "    healthy consumers lost   {:>8} under {}   vs {:>8} under {}",
            a.healthy_gaps(),
            a.policy.label(),
            b.healthy_gaps(),
            b.policy.label()
        );
        println!(
            "    stalled consumer lost    {:>8} under {}   vs {:>8} under {}",
            a.stalled_gaps(),
            a.policy.label(),
            b.stalled_gaps(),
            b.policy.label()
        );
        println!(
            "    publisher achieved       {:>8} under {}   vs {:>8} under {}",
            a.achieved_rate,
            a.policy.label(),
            b.achieved_rate,
            b.policy.label()
        );
        println!("{}", "-".repeat(72));
        println!(
            "\n  Neither policy is correct in general. drop_new keeps one sick consumer\n  \
             from affecting anyone else, and that consumer permanently loses events.\n  \
             block loses nothing, and lets one sick consumer throttle the publisher\n  \
             and therefore every other consumer on the stream.\n\n  \
             Which you want is a product decision. Felix makes it explicit rather\n  \
             than picking for you."
        );
    }

    println!(
        "\n  Measured single-node over loopback at fanout {}. These numbers say nothing\n  \
         about behaviour at thousands of subscribers or across a real network.\n  \
         Lost events are gone: Felix is at-most-once today, with no replay.\n",
        outcomes.first().map(|o| o.subscribers.len()).unwrap_or(0)
    );
}
