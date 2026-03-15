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
  nationality: "",
  countryOfBirth: "",
  yearsActive: null,
  era: "modern",
  position: null,
  teams: [],
  metadata: {
    nhlId: String(player.id),
    source: "@nhl-api/players",
  },
}));

process.stdout.write(JSON.stringify(normalizedPlayers));
