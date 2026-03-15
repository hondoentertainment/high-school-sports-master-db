import nhlPlayersModule from "@nhl-api/players";

const rawPlayers = Array.isArray(nhlPlayersModule)
  ? nhlPlayersModule
  : nhlPlayersModule?.default;

const players = Array.isArray(rawPlayers) ? rawPlayers : [];

const normalizedPlayers = players.map((player) => ({
  id: `a-nhl-${player.id}`,
  name: player.name,
  league: "NHL",
  sport: "hockey",
  nationality: null,
  countryOfBirth: null,
  yearsActive: null,
  era: null,
  position: null,
  teams: [],
  metadata: {
    nhlId: String(player.id),
    source: "@nhl-api/players",
  },
}));

process.stdout.write(JSON.stringify(normalizedPlayers));
