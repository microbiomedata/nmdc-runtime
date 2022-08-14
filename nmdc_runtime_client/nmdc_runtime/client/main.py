import click


@click.command()
@click.option("--config", "-c", help="path to config file")
def main(config):
    print(config)
