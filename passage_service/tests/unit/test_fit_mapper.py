from app.infra.tagging.fit.mapper import FitMapper


def test_fit_mapper_returns_scores() -> None:
    mapper = FitMapper(
        {
            "fit_scores": {
                "demo": {
                    "weights": {
                        "single_center_strength": 0.5,
                        "summary_strength": 0.5,
                    }
                }
            }
        }
    )
    scores = mapper.compute({"single_center_strength": 0.8, "summary_strength": 0.6})
    assert scores["demo"] == 0.7
