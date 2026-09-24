ALTER TABLE tenant_signing_keys
    ALTER COLUMN private_pem TYPE BYTEA USING decode(
        replace(replace(private_pem, '-', '+'), '_', '/') || repeat('=', (4 - length(private_pem) % 4) % 4),
        'base64'
    ),
    ALTER COLUMN public_pem TYPE BYTEA USING decode(
        replace(replace(public_pem, '-', '+'), '_', '/') || repeat('=', (4 - length(public_pem) % 4) % 4),
        'base64'
    );
