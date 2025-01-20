#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import os
import shutil
import subprocess
from abc import ABC
from contextlib import contextmanager
from datetime import datetime
from typing import Any, ClassVar, List, Optional, Tuple, cast

from dagger import Container, ExecError

from pipelines.airbyte_ci.connectors.context import ConnectorContext
from pipelines.helpers.utils import dagger_directory_as_zip_file
from pipelines.models.artifacts import Artifact
from pipelines.models.steps import Step, StepResult, StepStatus


class GradleTask(Step, ABC):
    """
    A step to run a Gradle task.

    Attributes:
        title (str): The step title.
        gradle_task_name (str): The Gradle task name to run.
        bind_to_docker_host (bool): Whether to install the docker client and bind it to the host.
        mount_connector_secrets (bool): Whether to mount connector secrets.
    """

    context: ConnectorContext

    STATIC_GRADLE_OPTIONS = ("--build-cache", "--scan")
    gradle_task_name: ClassVar[str]

    with_test_artifacts: ClassVar[bool] = False
    accept_extra_params = True

    @property
    def gradle_task_options(self) -> Tuple[str, ...]:
        if self.context.s3_build_cache_access_key_id and self.context.s3_build_cache_secret_key:
            return self.STATIC_GRADLE_OPTIONS + (f"-Ds3BuildCachePrefix={self.context.connector.technical_name}",)
        return self.STATIC_GRADLE_OPTIONS

    def _get_gradle_command(self, task: str, *args: Any, task_options: Optional[List[str]] = None) -> str:
        task_options = task_options or []
        return f"./gradlew {' '.join(self.gradle_task_options + args)} {task} {' '.join(task_options)}"

    def check_system_requirements(self):
        """
        Check if the system has all the required commands in the path.
        This could be improved to check for more specific versions of the commands.
        """
        required_commands_in_path = ["docker", "gradle", "jq", "xargs", "java"]
        for command in required_commands_in_path:
            if not shutil.which(command):
                raise ValueError(f"Command {command} is not in the path")

    @property
    def gradle_command(self):
        connector_gradle_task = f":airbyte-integrations:connectors:{self.context.connector.technical_name}:{self.gradle_task_name}"
        return self._get_gradle_command(connector_gradle_task)

    @contextmanager
    def gradle_environment(self):
        """
        Context manager to set the gradle environment:
        - Check if the system has all the required commands in the path.
        - Set the S3 build cache environment variables if available.
        - ... Add whatever setup/teardown logic needed to run a gradle task.
        """

        try:
            # Check if the system has all the required commands in the path.
            self.check_system_requirements()
            # Set the S3 build cache environment variables if available.
            if self.context.s3_build_cache_access_key_id and self.context.s3_build_cache_secret_key:
                os.environ["S3_BUILD_CACHE_ACCESS_KEY_ID"] = self.context.s3_build_cache_access_key_id.value
                os.environ["S3_BUILD_CACHE_SECRET_KEY"] = self.context.s3_build_cache_secret_key.value
            # Add secrets to the secrets folders
            secrets_dir = f"{self.context.connector.code_directory}/secrets"
            for secret in self.secrets:
                secret_path = f"{secrets_dir}/{secret.file_name}"
                os.makedirs(os.path.dirname(secret_path), exist_ok=True)
                secret_path.write_text(secret.value)
            yield
        finally:
            if self.context.s3_build_cache_access_key_id and self.context.s3_build_cache_secret_key:
                del os.environ["S3_BUILD_CACHE_ACCESS_KEY_ID"]
                del os.environ["S3_BUILD_CACHE_SECRET_KEY"]
            # Remove secrets from the secrets folders only in CI
            if self.context.is_ci:
                for secret in self.secrets:
                    secret_path = f"{secrets_dir}/{secret.file_name}"
                    os.remove(secret_path)

    def _run_gradle_in_subprocess(self) -> Tuple[str, str, int]:
        """
        Run a gradle command in a subprocess.
        """
        process = subprocess.run(self.gradle_command, shell=True, capture_output=True, text=True)
        if process.returncode != 0:
            stderr = f"Error while running gradle command: {self.gradle_command}" + process.stderr
            return process.stdout, stderr, process.returncode

        return process.stdout, process.stderr, process.returncode

    async def _run(self, *args: Any, **kwargs: Any) -> StepResult:
        try:
            with self.gradle_environment():
                stdout, stderr, returncode = self._run_gradle_in_subprocess()
                artifacts = []
                if self.with_test_artifacts:
                    if test_logs := await self._collect_test_logs():
                        artifacts.append(test_logs)
                    if test_results := await self._collect_test_results():
                        artifacts.append(test_results)
                step_result = StepResult(
                    step=self,
                    status=StepStatus.SUCCESS if returncode == 0 else StepStatus.FAILURE,
                    stdout=stdout,
                    stderr=stderr,
                    output=self.context.dagger_client.host().directory(str(self.context.connector.code_directory)),
                    artifacts=artifacts,
                )
                return step_result
        except Exception as e:
            error = e
            breakpoint()

    async def _collect_test_logs(self) -> Optional[Artifact]:
        """
        Exports the java docs to the host filesystem as a zip file.
        The docs are expected to be in build/test-logs, and will end up test-artifact directory by default
        One can change the destination directory by setting the outputs
        """
        test_logs_dir_name_in_container = "test-logs"
        test_logs_dir_name_in_zip = f"test-logs-{datetime.fromtimestamp(cast(float, self.context.pipeline_start_timestamp)).isoformat()}-{self.context.git_branch}-{self.gradle_task_name}".replace(
            "/", "_"
        )
        if (
            test_logs_dir_name_in_container
            not in await self.dagger_client.host().directory(f"{self.context.connector.code_directory}/build").entries()
        ):
            self.context.logger.warn(f"No {test_logs_dir_name_in_container} found directory in the build folder")
            return None
        try:
            zip_file = await dagger_directory_as_zip_file(
                self.dagger_client,
                await self.dagger_client.host().directory(
                    f"{self.context.connector.code_directory}/build/{test_logs_dir_name_in_container}"
                ),
                test_logs_dir_name_in_zip,
            )
            return Artifact(
                name=f"{test_logs_dir_name_in_zip}.zip",
                content=zip_file,
                content_type="application/zip",
                to_upload=True,
            )
        except ExecError as e:
            self.context.logger.error(str(e))
        return None

    async def _collect_test_results(self) -> Optional[Artifact]:
        """
        Exports the junit test results into the host filesystem as a zip file.
        The docs in the container are expected to be in build/test-results, and will end up test-artifact directory by default
        Only the XML files generated by junit are downloaded into the host filesystem
        One can change the destination directory by setting the outputs
        """
        test_results_dir_name_in_container = "test-results"
        test_results_dir_name_in_zip = f"test-results-{datetime.fromtimestamp(cast(float, self.context.pipeline_start_timestamp)).isoformat()}-{self.context.git_branch}-{self.gradle_task_name}".replace(
            "/", "_"
        )
        if (
            test_results_dir_name_in_container
            not in await self.dagger_client.host().directory(f"{self.context.connector.code_directory}/build").entries()
        ):
            self.context.logger.warn(f"No {test_results_dir_name_in_container} found directory in the build folder")
            return None
        try:
            zip_file = await dagger_directory_as_zip_file(
                self.dagger_client,
                await self.dagger_client.host().directory(
                    f"{self.context.connector.code_directory}/build/{test_results_dir_name_in_container}"
                ),
                test_results_dir_name_in_zip,
            )
            return Artifact(
                name=f"{test_results_dir_name_in_zip}.zip",
                content=zip_file,
                content_type="application/zip",
                to_upload=True,
            )
        except ExecError as e:
            self.context.logger.error(str(e))
            return None
