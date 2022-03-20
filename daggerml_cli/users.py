import getpass
import typer

app = typer.Typer()

@app.command()
def get_credentials(
    username: str = typer.Option(..., help="Your DaggerML username.", envvar="USER")
):
    """
    Authenticate user and print API credentials to stdout.
    """
    password = getpass.getpass(f"Password for {username}: ")
    typer.echo(f"auth complete {username}:{password}")

if __name__ == "__main__":
    app()
