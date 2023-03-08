from prefect import flow, get_run_logger
from prefect_shell import shell_run_command
from prefect_gcp import GcpCredentials
from prefect_gcp.secret_manager import read_secret
from prefect_slack import SlackWebhook
from prefect_slack.messages import send_incoming_webhook_message


@flow
def deploy_databox():
    logger = get_run_logger()
    logger.info('starting data-workflow gcp deploy')
    startup_adhoc_f = shell_run_command.with_options(name='statup-mode adhoc')(
        command = 'gcloud compute instances add-metadata data-box --zone us-east1-b --metadata statup-mode="adhoc"', 
        return_all = True,
    )
    spinup_databox_f = shell_run_command.with_options(name='spinup data-box')(
        command = "gcloud compute instances start data-box --zone us-east1-b", 
        return_all = True,
        wait_for = [startup_adhoc_f]
    )
    config_ssh_rsync_f = shell_run_command.with_options(name='config-ssh & rsync', retries=3, retry_delay_seconds=3)(
        command = "gcloud compute config-ssh && rsync --recursive --compress --delete --rsh=ssh /Users/bobcolner/Instasize/data-workflows/data-workflow/ data-box.us-east1-b.emerald-skill-201716:/home/bobcolner/data-workflow",
        return_all = True,
        wait_for = [spinup_databox_f],
    )
    spindown_databox_f = shell_run_command.with_options(name='spindown data-box')(
        command = "gcloud compute instances stop data-box --zone us-east1-b", 
        return_all = True, 
        wait_for = [config_ssh_rsync_f],
    )
    startup_workflow_spindown_f = shell_run_command.with_options(name='statup-mode workflow_spindown')(
        command = 'gcloud compute instances add-metadata data-box --zone us-east1-b --metadata statup-mode="workflow_spindown"', 
        return_all = True,
        wait_for = [config_ssh_rsync_f],
    )
    latest_git_commit_hash = shell_run_command.with_options(name='latest git commit hash')(
        command = 'git rev-parse HEAD | cut -c 1-8',
        return_all = True,
        wait_for = [spindown_databox_f],
    )
    output_msg = f"Sucessfully deployed data-workflow version: {latest_git_commit_hash[0]}"
    logger.info(output_msg)
    gcp_credentials = GcpCredentials(project='emerald-skill-201716')
    webhook_url = read_secret('slack_webhook_url', gcp_credentials)
    send_incoming_webhook_message(
        slack_webhook=SlackWebhook(url=webhook_url),
        text=f'{output_msg} :sparkles:',
        wait_for=[spindown_databox_f],
    )


if __name__ == "__main__":
    deploy_databox()
