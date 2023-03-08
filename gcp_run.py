import re
import os
import timeit
import logging
from prefect.executors import DaskExecutor
import flow_etl


try:
    # https://googleapis.dev/python/logging/latest/stdlib-usage.html
    # https://cloud.google.com/logging/docs/setup/python
    from google.cloud import logging as gcloud_logging
    client = gcloud_logging.Client()
    handler = gcloud_logging.handlers.CloudLoggingHandler(client, name="data-workflow")
    logging.getLogger().setLevel(logging.DEBUG)  # defaults to WARN, DEBUG, INFO
    gcloud_logging.handlers.setup_logging(handler)  # get root python logger
except ImportError as exc:
    logging.error(exc)
    pass


def main():
    # check instance meta-data for starup-mode param
    startup_mode = (
        os.popen(
            "gcloud compute instances describe data-box --zone us-east1-b --flatten='metadata[statup-mode]'"
        )
        .read()
        .split()[1]
    )
    logging.info(f'gcp_run: ELT workflow startup-mode: {startup_mode}')

    if startup_mode == "adhoc":
        print("Adhoc testing mode")
    elif bool(re.search('workflow', startup_mode)):
        print("Workflow mode")
        logging.info("gcp_run: ELT workflow Starting")
        try:
            start_time = timeit.default_timer()
            executor = DaskExecutor(
            cluster_kwargs={
                'processes': True,
                'n_workers': 4,
                'threads_per_worker': 4,
                'dashboard_address': None,
                }
            )
            flow_state = flow_etl.flow.run(executor=executor)
        except Exception as exc:
            logging.error(exc)
            pass

        logging.info(f"gcp_run: ELT workflow Done. Run time: {(timeit.default_timer() - start_time) / 60} mins")

    # immediately shutdown VM only if in 'spindown' mode
    if bool(re.search('spindown', startup_mode)):
        logging.info("gcp_run: shuting down VM")
        os.system("gcloud compute instances stop data-box --zone us-east1-b")


if __name__ == '__main__':
    main()
