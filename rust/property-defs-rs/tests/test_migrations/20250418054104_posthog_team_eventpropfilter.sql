-- Add migration script here

CREATE TYPE property_type AS ENUM('event', 'person', 'group', 'session')

CREATE TABLE IF NOT EXISTS posthog_team_eventpropfilter (
  team_id       bigint          NOT NULL,
  prop_type     
  filter        bytea           NOT NULL,
  created_at    timestampz      NOT NULL,
  updated_at    timestampz      NOT NULL,

  UNIQUE(team_id, prop_type)
)


