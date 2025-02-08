# import dagster as dg


# @dg.asset(
#     op_tags={"operation": "example"},
#     partitions_def=dg.DailyPartitionsDefinition("2024-01-01"),
# )
# def example_asset(context: dg.AssetExecutionContext):
#     context.log.info(context.partition_key)


# partitioned_asset_job = dg.define_asset_job("partitioned_job", selection=[example_asset])

# defs = dg.Definitions(assets=[example_asset], jobs=[partitioned_asset_job])

# import shutil

# from dagster import (
#     AssetCheckSpec,
#     AssetExecutionContext,
#     Definitions,
#     PipesSubprocessClient,
#     asset,
#     file_relative_path,
# )


# @asset(
#     check_specs=[AssetCheckSpec(name="no_empty_order_check", asset="subprocess_asset")],
# )
# def subprocess_asset(
#     context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
# ):
#     cmd = [
#         shutil.which("python"),
#         file_relative_path(__file__, "external_code.py"),
#     ]
#     return pipes_subprocess_client.run(
#         command=cmd, context=context
#     ).get_materialize_result()


# defs = Definitions(
#     assets=[subprocess_asset],
#     resources={"pipes_subprocess_client": PipesSubprocessClient()},
# )






# import subprocess
# import sys

# from dagster import (
#     AssetExecutionContext,
#     asset,
#     Config,
#     Definitions,
#     ExperimentalPipesContext,
#     PipesSubprocessClient,
# )
# from dagster._core.pipes import execute_command_process


# class PipesConfig(Config):
#     message: str


# class PipesStepExecutionContext(ExperimentalPipesContext, AssetExecutionContext):
#     pass


# @asset
# def subprocess_asset(context: PipesStepExecutionContext, config: PipesConfig):
#     pipes_subprocess_client = PipesSubprocessClient(
#         env={"MY_CUSTOM_ENV_VAR": "some_value"}
#     )  # Added env vars

#     result = pipes_subprocess_client.run(
#         context=context,
#         #command=["python", "my_script.py", "--message", config.message],
#         command=["python", "external_code.py", "--message", config.message],
#         #Added redirection of stdout and stderr
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE
#     )

#     #Check standard output
#     print("Standard output:", result.stdout)
#     #Check standard error
#     print("Standard error:", result.stderr)


#     if result.return_code != 0:
#         raise Exception(f"Subprocess failed with code: {result.return_code}")

#     # Return a value to materialize (or not if not needed)
#     return f"Executed with message: {config.message}"

# @asset
# def subprocess_asset_with_execute_command_process(context: ExperimentalPipesContext, config: PipesConfig):
#     process = execute_command_process(
#         context=context,
#         #command=["python", "my_script.py", "--message", config.message],
#         command=["python", "external_code.py", "--message", config.message],
#         #Added redirection of stdout and stderr
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE
#     )

#     #Check standard output
#     print("Standard output:", process.stdout)
#     #Check standard error
#     print("Standard error:", process.stderr)


#     if process.returncode != 0:
#         raise Exception(f"Subprocess failed with code: {process.returncode}")

#     # Return a value to materialize (or not if not needed)
#     return f"Executed with message: {config.message}"

# defs = Definitions(
#     assets=[subprocess_asset],
#     resources={
#        "pipes_context": PipesStepExecutionContext,
#     },
#     jobs=[]
# )



# definitions.py

# import shutil

# from dagster import (
#     AssetCheckSpec,
#     AssetExecutionContext,
#     Definitions,
#     PipesSubprocessClient,
#     asset,
#     file_relative_path,
# )

# @asset(
#     check_specs=[AssetCheckSpec(name="no_empty_order_check", asset="subprocess_asset")],
# )
# def subprocess_asset(
#     context: AssetExecutionContext
# ):
#     pipes_subprocess_client = PipesSubprocessClient()
#     cmd = [
#         shutil.which("python"),
#         file_relative_path(__file__, "external_code.py"),
#     ]
#     return pipes_subprocess_client.run(
#         command=cmd, context=context
#     ).get_materialize_result()


# defs = Definitions(
#     assets=[subprocess_asset],
#     resources={"pipes_subprocess_client": PipesSubprocessClient()},
# )


# import shutil
# import subprocess  # Import the subprocess module

# from dagster import (
#     AssetCheckSpec,
#     AssetExecutionContext,
#     Definitions,
#     PipesSubprocessClient,
#     asset,
#     file_relative_path,
# )

# @asset(
#     check_specs=[AssetCheckSpec(name="no_empty_order_check", asset="subprocess_asset")],
# )
# def subprocess_asset(
#     context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
# ):
#     cmd = [
#         shutil.which("python"),
#         file_relative_path(__file__, "external_code.py"),
#     ]
#     try:
#         result = pipes_subprocess_client.run(
#             command=cmd, context=context, stdout=subprocess.PIPE, stderr=subprocess.PIPE
#         )

#         # Log the standard output and standard error
#         context.log.info(f"Subprocess stdout: {result.stdout}")
#         context.log.info(f"Subprocess stderr: {result.stderr}")

#         if result.return_code != 0:
#             raise Exception(
#                 f"Subprocess failed with code: {result.return_code}, stderr: {result.stderr}"
#             )

#         return result.get_materialize_result()

#     except Exception as e:
#         context.log.error(f"Error running subprocess: {e}")
#         raise
    

# defs = Definitions(
#     assets=[subprocess_asset],
#     resources={"pipes_subprocess_client": PipesSubprocessClient()},
# )


# import shutil

# from dagster import (
#     AssetCheckSpec,
#     AssetExecutionContext,
#     Definitions,
#     PipesSubprocessClient,
#     asset,
#     file_relative_path,
# )

# @asset(
#     check_specs=[AssetCheckSpec(name="no_empty_order_check", asset="subprocess_asset")],
# )
# def subprocess_asset(
#     context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
# ):
#     cmd = [
#         shutil.which("python"),
#         file_relative_path(__file__, "external_code.py"),
#     ]
#     try:
#         result = pipes_subprocess_client.run(
#             command=cmd, context=context
#         )

#         if result.return_code != 0:
#             raise Exception(
#                 f"Subprocess failed with code: {result.return_code}"
#             )

#         return result.get_materialize_result()

#     except Exception as e:
#         context.log.error(f"Error running subprocess: {e}")
#         raise
    

# defs = Definitions(
#     assets=[subprocess_asset],
#     resources={"pipes_subprocess_client": PipesSubprocessClient()},
# )



import shutil
import sys

#from MM_WECHSEL import mm_wechsel_job
from MM_BIL import mm_bil_job

from dagster import (
    AssetCheckSpec,
    AssetExecutionContext,
    Definitions,
    PipesSubprocessClient,
    asset,
    file_relative_path,
)

@asset(
    check_specs=[AssetCheckSpec(name="no_empty_order_check", asset="subprocess_asset")],
)
def subprocess_asset(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
):
    cmd = [
        sys.executable,  # Use sys.executable to ensure correct Python interpreter
        file_relative_path(__file__, "external_code.py"),
    ]
    
    context.log.info(f"Executing command: {' '.join(cmd)}")
    
    try:
        result = pipes_subprocess_client.run(
            command=cmd, 
            context=context
        )
        return result.get_materialize_result()
    except Exception as e:
        context.log.error(f"Subprocess execution failed: {e}")
        raise

defs = Definitions(
    assets=[subprocess_asset],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)

# defs = Definitions(
#     jobs=[mm_bil_job, mm_wechsel_job],
# )

defs = Definitions(
    jobs=[mm_bil_job],
)

# defs = Definitions(
#     jobs=[mm_wechsel_job],
# )