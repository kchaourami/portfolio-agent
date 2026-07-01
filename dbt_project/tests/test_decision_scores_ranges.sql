/*
Test custom : vérifie que les scores du Decision Engine sont dans des
plages réalistes, et que la décision appartient bien à l'échelle prévue.

Convention dbt : ce test doit retourner 0 ligne pour passer.
S'il retourne des lignes, ce sont des anomalies à investiguer.
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