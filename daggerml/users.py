import typer
import getpass
from daggerml.util import print_result


_app = typer.Typer(result_callback=print_result)


@_app.command()
def get_credentials(username: str):
    """Authenticate user and print API credentials to stdout.

    Parameters
    ----------
    username : str
        the daggerml username

    Returns
    -------
    boolean : successful login?
    """
    password = getpass.getpass(f"Password for {username}: ")
    return {'success': isinstance(password, str)}
