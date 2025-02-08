# import pandas as pd
# from dagster_pipes import PipesContext, open_dagster_pipes


# def main():
#     orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
#     total_orders = len(orders_df)
#     # get the Dagster Pipes context
#     context = PipesContext.get()
#     # send structured metadata back to Dagster
#     context.report_asset_materialization(metadata={"total_orders": total_orders})
#     # report data quality check result back to Dagster
#     context.report_asset_check(
#         passed=orders_df[["item_id"]].notnull().all().bool(),
#         check_name="no_empty_order_check",
#     )


# if __name__ == "__main__":
#     # connect to Dagster Pipes
#     with open_dagster_pipes():
#         main()




# import argparse
# import os
# import sys

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--message", required=True)
#     args = parser.parse_args()

#     message = args.message
#     print(f"Received message: {message}")

#     # Simulate an error if the message is "error"
#     if message == "error":
#         print("Simulating an error...")
#         sys.stderr.write("This is an error message to stderr.\n") #write to standard error
#         sys.exit(2)  # Exit with error code 2

#     print("Script completed successfully.")


# import pandas as pd
# from dagster_pipes import PipesContext, open_dagster_pipes
# import logging  # Import the logging module

# def main():
#     orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
#     total_orders = len(orders_df)

#     # Get the Dagster Pipes context
#     context = PipesContext.get()

#     # Log messages using PipesContext.log
#     context.log.info(f"Total orders: {total_orders}")
#     context.log.debug("This is a debug message")
#     context.log.warning("This is a warning message")
#     context.log.error("This is an error message (if something went wrong)")

#     # Send structured metadata back to Dagster
#     context.report_asset_materialization(metadata={"total_orders": total_orders})

#     # Report data quality check result back to Dagster
#     context.report_asset_check(
#         passed=orders_df[["item_id"]].notnull().all().bool(),
#         check_name="no_empty_order_check",
#     )

#     # Simulate an error (for testing)
#     # if True:
#     #     raise ValueError("Simulated error in external_code.py")


# if __name__ == "__main__":
#     # Connect to Dagster Pipes
#     with open_dagster_pipes():
#         # Configure logging (optional, but good practice)
#         logging.basicConfig(level=logging.INFO) # Or logging.DEBUG
#         main()




# import os
# import logging
# from dagster_pipes import PipesContext, open_dagster_pipes
# import pandas as pd

# def main():
#     try:
#         context = PipesContext.get()
#         logging.basicConfig(level=logging.DEBUG)
#         context.log.info("Starting external_code.py")

#         # Print environment variables
#         for key, value in os.environ.items():
#             context.log.debug(f"Env var: {key} = {value}")

#         orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
#         total_orders = len(orders_df)
#         # get the Dagster Pipes context

#         # send structured metadata back to Dagster
#         context.report_asset_materialization(metadata={"total_orders": total_orders})
#         # report data quality check result back to Dagster
#         context.report_asset_check(
#             passed=orders_df[["item_id"]].notnull().all().bool(),
#             check_name="no_empty_order_check",
#         )

#     except Exception as e:
#         logging.exception("An unexpected error occurred in external_code.py:")
#         if 'context' in locals():  # Check if context is defined
#             context.log.error(f"Exception: {e}")
#         else:
#             print(f"Exception before context initialization: {e}") #if pipes context can't connect, use standard logging


# if __name__ == "__main__":
#     # connect to Dagster Pipes
#     with open_dagster_pipes():
#         # Configure logging (optional, but good practice)
#         logging.basicConfig(level=logging.INFO) # Or logging.DEBUG
#         main()




import sys
import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    total_orders = len(orders_df)
    
    # get the Dagster Pipes context
    context = PipesContext.get()
    
    # send structured metadata back to Dagster
    context.report_asset_materialization(metadata={"total_orders": total_orders})
    
    # report data quality check result back to Dagster
    context.report_asset_check(
        passed=orders_df[["item_id"]].notnull().all().bool(),
        check_name="no_empty_order_check",
    )

if __name__ == "__main__":
    # connect to Dagster Pipes
    with open_dagster_pipes():
        main()