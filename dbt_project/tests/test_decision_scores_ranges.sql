/*
Test custom : vérifie que les scores du Decision Engine sont dans des
plages réalistes, et que la décision appartient bien à l'échelle prévue.

Convention dbt : ce test DOIT retourner 0 ligne pour passer.
S'il retourne des lignes, ce sont des anomalies à investiguer.

Particularité par rapport à test_risk_signals_ranges.sql : la table
`decisions` n'est PAS un modèle dbt — elle est écrite directement par
decision_engine.py (Python, via INSERT), pas par une transformation SQL
dbt. On la référence donc par son nom de table brut (main.decisions),
sans passer par la fonction ref() de dbt — ce test n'apparaît donc pas
dans le graphe de lignage dbt, mais reste bien exécuté à chaque
dbt test. Il faut que `decisions` ait déjà été peuplée par un run du
pipeline avant de lancer ce test.

Seuils choisis :
  confidence_score, risk_score, momentum_score, macro_score : tous
    définis sur une échelle 0-100 dans decision_models.py — toute valeur
    hors de cet intervalle indique une anomalie de calcul
  decision : doit appartenir aux 6 valeurs de l'enum Decision
    (decision_models.py) — toute autre valeur indique une corruption de
    données ou un bug d'écriture
*/

select
    decision_id,
    ticker,
    decision,
    confidence_score,
    risk_score,
    momentum_score,
    macro_score,
    'valeur_hors_plage' as anomalie

from main.decisions

where
    -- Les 4 scores doivent être dans [0, 100]
    (confidence_score < 0 or confidence_score > 100)
    or (risk_score < 0 or risk_score > 100)
    or (momentum_score < 0 or momentum_score > 100)
    or (macro_score < 0 or macro_score > 100)

    -- La décision doit appartenir à l'échelle prévue
    or decision not in (
        'BUY_WATCH', 'INCREASE', 'HOLD', 'WATCH', 'REDUCE', 'SELL_SIGNAL'
    )