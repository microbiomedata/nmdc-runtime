from subprocess import run

from invoke import task


@task
def publish(c):
    completed = run(
        "git describe --tags $(git rev-list --tags --max-count=1)",
        shell=True,
        capture_output=True,
        encoding="utf-8",
    )
    vtag = completed.stdout.strip()
    major_minor, dot, patch = vtag.rpartition(".")
    vtag_new = major_minor + dot + str(int(patch) + 1)
    run(["git", "tag", vtag_new])
    run(["git", "push"])
    run(["git", "push", "--tags"])
    c.run("rm dist/*.*", warn=True)
    c.run("python setup.py sdist bdist_wheel")
    c.run("twine upload dist/*")
