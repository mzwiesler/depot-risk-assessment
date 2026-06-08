from depot_risk_assessment.providers._base import ETFProvider, _finalize, get_provider, register  # noqa: F401

# Import providers to trigger self-registration
import depot_risk_assessment.providers.amundi  # noqa: F401
import depot_risk_assessment.providers.invesco  # noqa: F401
import depot_risk_assessment.providers.ishares  # noqa: F401
import depot_risk_assessment.providers.hanetf  # noqa: F401
import depot_risk_assessment.providers.xtrackers  # noqa: F401
