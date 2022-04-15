import daggerml.users as users  # noqa: F401
import daggerml.dags as dags  # noqa: F401
import daggerml.data as data  # noqa: F401
import daggerml.util as util  # noqa: F401
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution("daggerml").version
except DistributionNotFound:
    __version__ = 'local'

del get_distribution, DistributionNotFound
