CREATE TABLE IF NOT EXISTS visuals.treasury_yield_curve (
    date DATE,
    instrument varchar(255),
    yield float8,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(date, instrument)
);
