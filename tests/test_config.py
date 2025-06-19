import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml_cli.config import Config, ConfigError
from daggerml_cli.repo import Ref


class TestConfig(TestCase):
    def test_ephemeral_config(self):
        """A non-persistent Config object which does not load its state from or
        write its state to files."""

        # An instance without a CONFIG_DIR or PROJECT_DIR -- it can't read or
        # write files.
        config = Config()

        # Trying to access a key that hasn't been set yet is an error.
        with self.assertRaises(ConfigError):
            assert config.BRANCH is None

        # Set BRANCH key without writing to any files, and then access it.
        config._BRANCH = "master"
        assert config.BRANCH == "master"

        # Environment variables can provide a value, prefix the desired config
        # field name with "DML_" as the name of the environment variable.
        with mock.patch.dict(os.environ, {"DML_REPO": "test0"}):
            assert config.REPO == "test0"

            # And REPO can be modified, but it won't be persisted because we are
            # not using the Config object as a context manager.
            config.REPO = "test1"
            assert config.REPO == "test1"

        # Some fields access other fields internally -- these are dynamic, or
        # computed fields. It's an error to access them if the fields they
        # access haven't been set. For example, REPO_DIR internally references
        # CONFIG_DIR which hasn't been set yet.
        with self.assertRaises(ConfigError):
            config.REPO_DIR  # noqa:B018

        # Setting a dynamic (computed) field is an error.
        with self.assertRaises(AttributeError):
            config.REPO_DIR = "/not/a/real/path"

        # Assigning a value to CONFIG_DIR allows us to access REPO_DIR. Note
        # that CONFIG_DIR is fake; the directory doesn't exist and nothing can
        # actually be read from or written to there.
        config._CONFIG_DIR = "/foop"
        assert config.REPO_DIR == "/foop/repo"

    def test_persistent_config(self):
        """A persistent Config object which loads its state from and writes its
        state to files."""

        with TemporaryDirectory() as config_dir:
            with TemporaryDirectory() as project_dir:
                # Persistence requires at least a CONFIG_DIR and a PROJECT_DIR,
                # and writes are performed in a context manager. The context
                # manager ensures that nothing is persisted when an exception is
                # raised, as this could result in a partial configuration being
                # written.
                with Config(config_dir, project_dir) as config:
                    # Accessing a field which hasn't been set is still an error.
                    with self.assertRaises(ConfigError):
                        assert config.REPO is None

                    # Set and store REPO field so it can be accessed without errors.
                    config.REPO = "test0"
                    assert config.REPO == "test0"

                    # Set and store the rest of the configurable fields.
                    config.BRANCH = "master"
                    config.USER = "rupert"

                    # Verify the stored and dynamic (computed) fields are correct.
                    assert config.BRANCH == "master"
                    assert config.USER == "rupert"
                    assert config.BRANCHREF == Ref(to="head/master")
                    assert config.REPO_DIR == f"{config_dir}/repo"
                    assert config.REPO_PATH == f"{config_dir}/repo/test0"

                # Load a persistent Config object by providing CONFIG_DIR and
                # PROJECT_DIR to the dataclass constructor. We don't need the
                # context manager because we are only reading the configuration.
                config = Config(config_dir, project_dir)
                assert config.REPO == "test0"
                assert config.BRANCH == "master"
                assert config.USER == "rupert"
                assert config.BRANCHREF == Ref(to="head/master")
                assert config.REPO_DIR == f"{config_dir}/repo"
                assert config.REPO_PATH == f"{config_dir}/repo/test0"
