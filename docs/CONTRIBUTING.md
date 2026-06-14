## Contributing

1. Crée une branche basée sur `main` : `git checkout -b feat/ma-feature`
2. Mets à jour `.env` localement en copiant `.env.example`.
3. Respecte le style de commits : `Commit X — Sujet: bref résumé`.
4. Tests : exécute `dbt test` et les scripts unitaires si présents.
5. Ouvre une merge request et décris le changement + instructions de test.

Variables secrètes : ne pas committer `.env` — il est déjà dans `.gitignore`.
