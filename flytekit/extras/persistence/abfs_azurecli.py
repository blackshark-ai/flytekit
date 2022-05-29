# Copyright (C) 2015-2022 Blackshark.ai GmbH. All Rights reserved. www.blackshark.ai

import logging
import os
import os as _os
import re as _re
import time
import typing
from pathlib import Path
from shutil import which as shell_which
from typing import List

from flytekit.configuration import AzureBlobConfig, DataConfig
from flytekit.core.data_persistence import DataPersistence, DataPersistencePlugins
from flytekit.exceptions.user import FlyteUserException
from flytekit.tools import subprocess

STORAGE_ACCOUNT_NAME_ENV_VAR = "AZURE_STORAGE_ACCOUNT"
STORAGE_ACCOUNT_KEY_ENV_VAR = "AZURE_STORAGE_KEY"


def _update_cmd_config_and_execute(cmd: List[str], azure_cfg: AzureBlobConfig) -> int:
    env = _os.environ.copy()

    if azure_cfg.storage_account_name:
        env[STORAGE_ACCOUNT_NAME_ENV_VAR] = azure_cfg.storage_account_name
    else:
        if not os.getenv(STORAGE_ACCOUNT_NAME_ENV_VAR):
            raise ValueError(
                'Azure storage account name not set! Can be set either in config file or as env var {0}'
                .format(STORAGE_ACCOUNT_NAME_ENV_VAR))

    if azure_cfg.storage_account_key:
        env[STORAGE_ACCOUNT_KEY_ENV_VAR] = azure_cfg.storage_account_key
    else:
        if not os.getenv(STORAGE_ACCOUNT_KEY_ENV_VAR):
            raise ValueError(
                'Azure storage account key not set! Can be set either in config file or as env var {0}'
                .format(STORAGE_ACCOUNT_KEY_ENV_VAR))

    retry = 0
    while True:
        try:
            return subprocess.check_call(cmd, env=env)
        except Exception as e:
            logging.error(f"Exception when trying to execute {cmd}, reason: {str(e)}")
            retry += 1
            if retry > azure_cfg.retries:
                raise
            secs = azure_cfg.backoff
            logging.info(f"Sleeping before retrying again, after {secs} seconds")
            time.sleep(secs.total_seconds())
            logging.info("Retrying again")


class AzurePersistence(DataPersistence):
    """
    DataPersistence plugin for Azure blob storage. Use azure-cli to manage the transfer. The binary needs to be
    installed separately.

    .. prompt::

       pip install azure-cli

    """

    PROTOCOL = "abfs://"
    _AZURE_CLI = "az"

    def __init__(self, default_prefix: typing.Optional[str] = None,
                 data_config: typing.Optional[DataConfig] = None):
        super().__init__(name="azurecli-abfs", default_prefix=default_prefix)
        self.azure_cfg = data_config.azure if data_config else AzureBlobConfig.auto()

    @staticmethod
    def _check_binary() -> None:
        if not shell_which(AzurePersistence._AZURE_CLI):
            raise FlyteUserException("az not found. Install it with `pip install azure-cli`.")

    @staticmethod
    def _check_uri(uri: str) -> None:
        if not uri.startswith(AzurePersistence.PROTOCOL):
            raise FlyteUserException(
                f"{uri} is not a valid Azure Blob url. Use the format {AzurePersistence.PROTOCOL}..."
            )

    @staticmethod
    def _split_abfs_path(path: str) -> typing.Tuple[str, str]:
        path = path[len(AzurePersistence.PROTOCOL):]
        first_slash = path.index("/")
        return path[:first_slash], path[first_slash + 1:]

    def _get_storage_account_url(self, container: str, blob_path: str) -> str:
        storage_account = self.azure_cfg.storage_account_key
        if not storage_account:
            storage_account = _os.environ["AZURE_STORAGE_ACCOUNT"]
        return f"https://{storage_account}.blob.core.windows.net/{container}/{blob_path}"

    def exists(self, path: str) -> bool:
        AzurePersistence._check_binary()
        AzurePersistence._check_uri(path)
        container, blob_path = self._split_abfs_path(path)

        cmd = [AzurePersistence._AZURE_CLI, "storage", "blob", "show", "--container", container, "--name",
               blob_path]

        try:
            _update_cmd_config_and_execute(azure_cfg=self.azure_cfg, cmd=cmd)
            return True
        except Exception as ex:
            if _re.search("ErrorCode:ContainerNotFound", str(ex)):
                return False
            if _re.search("ErrorCode:BlobNotFound", str(ex)):
                return False
            raise ex

    def get(self, from_path: str, to_path: str, recursive: bool = False) -> int:
        AzurePersistence._check_binary()
        AzurePersistence._check_uri(from_path)
        container, blob_path = self._split_abfs_path(from_path)

        cmd = [
            AzurePersistence._AZURE_CLI,
            "storage",
            "copy",
            "--source",
            self._get_storage_account_url(container=container, blob_path=blob_path),
            "--destination",
            to_path,
        ]

        if recursive:
            cmd.append("--recursive")

        cmd_result = _update_cmd_config_and_execute(azure_cfg=self.azure_cfg, cmd=cmd)

        # az copy storage downloads does not allow to download just a folders contents
        # we therefore need to move the folders contents to the parent folder
        if recursive:
            downloaded_dir = Path(to_path) / Path(blob_path).name
            for file_or_dir in downloaded_dir.glob("*"):
                file_or_dir.rename(file_or_dir.parent.parent.joinpath(file_or_dir.name))
        return cmd_result

    def put(self, from_path: str, to_path: str, recursive: bool = False) -> int:
        AzurePersistence._check_binary()
        AzurePersistence._check_uri(to_path)
        container, blob = self._split_abfs_path(to_path)

        if recursive:
            cmd = [
                AzurePersistence._AZURE_CLI,
                "storage",
                "copy",
                "--source",
                from_path + "/*",
                "--destination",
                self._get_storage_account_url(container=container, blob_path=blob),
                "--recursive",
            ]
        else:
            cmd = [
                AzurePersistence._AZURE_CLI,
                "storage",
                "copy",
                "--source",
                from_path,
                "--destination",
                self._get_storage_account_url(container=container, blob_path=blob),
            ]

        return _update_cmd_config_and_execute(azure_cfg=self.azure_cfg, cmd=cmd)

    def construct_path(self, add_protocol: bool, add_prefix: bool, *paths: str) -> str:
        paths = list(paths)  # make type check happy
        if add_prefix:
            paths.insert(0, self.default_prefix)
        path = "/".join(paths)
        if add_protocol:
            return f"{self.PROTOCOL}{path}"
        return path


DataPersistencePlugins.register_plugin(AzurePersistence.PROTOCOL, AzurePersistence)
