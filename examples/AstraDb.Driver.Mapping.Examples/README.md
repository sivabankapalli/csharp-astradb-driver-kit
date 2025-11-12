# ABC.AstraDB.SampleApp2 (updated)

Demonstrates **both** modes:
- **Mapped**: POCO methods via Cassandra.Mapping (default)
- **Raw**: non-POCO methods only (no IMapper registration)

Switch modes in `appsettings.json`:
```json
{ "Mode": "Mapped" } // or "Raw"
```

DI:
- `services.AddAstraDbDriver(section)` for **Raw**
- `services.AddAstraDbDriver(section, reg => { ... })` for **Mapped**
