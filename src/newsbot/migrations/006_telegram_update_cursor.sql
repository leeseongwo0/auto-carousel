CREATE TABLE telegram_update_cursors (
    stream TEXT PRIMARY KEY CHECK (stream = 'approval'),
    next_offset INTEGER NOT NULL CHECK (next_offset >= 0),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
