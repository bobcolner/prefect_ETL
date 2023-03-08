import datetime as dt
from prefect import Flow, unmapped
from prefect.utilities.notifications import slack_notifier
from prefect.engine.results import GCSResult
import tasks_sql
import tasks_aa
import tasks_asa
import tasks_qa
import tasks_rc
import tasks_gp


with Flow(
    name="data-workflow",
    result=GCSResult(bucket="data-workflow-checkpoints"),
    state_handlers=[slack_notifier(backend_info=False)],
) as flow:

    # revenue cat ETL
    get_latest_revenuecat_etl_file_f = tasks_rc.get_latest_revenuecat_etl_file()

    revenuecat_daily_etl_f = tasks_sql.template_revenuecat_daily_etl(
        latest_file=get_latest_revenuecat_etl_file_f
    )

    # app annie ETL
    # app_country_date_list = tasks_aa.get_app_country_date_list()

    # get_insert_keywords_f = tasks_aa.get_insert_keywords.map(
    #     app_country_date=app_country_date_list
    # )
    # dims_appannie_keywords_f = tasks_sql.dims_appannie_keywords(
    #     upstream_tasks=[get_insert_keywords_f]
    # )
    # asoc_daily_update_f = tasks_aa.asoc_daily_update()

    # apple search ads ETL
    asa_token_f = tasks_asa.asa_get_api_token()
    asa_params_f = tasks_asa.asa_get_params(
        start_date=(dt.date.today() - dt.timedelta(days=1)).isoformat(),
        end_date=dt.date.today().isoformat()
    )
    asa_get_app_date_f = tasks_asa.asa_get_app_date.map(
        param=asa_params_f,
        access_token=unmapped(asa_token_f)
    )
    asa_consolidation_f = tasks_sql.asa_consolidation(
        upstream_tasks=[asa_get_app_date_f],
    )

    # google play
    request_files = tasks_gp.gp_get_request_files()

    gp_get_and_put_csv_f = tasks_gp.gp_get_and_put_csv.map(
        file_name=request_files
    )
    dims_google_play_stats_f = tasks_sql.dims_google_play_stats(upstream_tasks=[gp_get_and_put_csv_f])

    # data ingestion table freshness QA
    qa_get_table_freshness_params_f = tasks_qa.qa_get_table_freshness_params()

    qa_check_table_freshness_f = tasks_qa.qa_check_table_freshness.map(
        param = qa_get_table_freshness_params_f,
        upstream_tasks=[
            # revenuecat_daily_etl_f,
            asa_consolidation_f,
            dims_google_play_stats_f, 
        ],
    )

    # core data transforms/views
    events_subscriptions_f = tasks_sql.events_subscriptions()

    dims_devices_install_instasize_f = tasks_sql.dims_devices_install_instasize()

    dims_devices_metrics_instasize_f = tasks_sql.dims_devices_metrics_instasize(
        upstream_tasks=[dims_devices_install_instasize_f]
    )
    dims_devices_instasize_f = tasks_sql.dims_devices_instasize(
        upstream_tasks=[
            dims_devices_metrics_instasize_f,
            dims_devices_install_instasize_f,
        ]
    )
    dims_devices_made_f = tasks_sql.dims_devices_made()

    dims_devices_videomade_f = tasks_sql.dims_devices_videomade()

    dims_devices_selfiemade_f = tasks_sql.dims_devices_selfiemade()

    dims_devices_typeloop_f = tasks_sql.dims_devices_typeloop()

    dims_devices_instasize_android_f = tasks_sql.dims_devices_instasize_android()

    dims_devices_made_android_f = tasks_sql.dims_devices_made_android()

    dims_prodsubs_ios_instasize_f = tasks_sql.dims_prodsubs_ios_instasize(
        upstream_tasks=[events_subscriptions_f]
    )
    dims_attribution_instasize_f = tasks_sql.dims_attribution_instasize(
        upstream_tasks=[
            events_subscriptions_f,
            dims_devices_instasize_f,    
        ]
    )
    dims_att_subs_instasize_f = tasks_sql.dims_att_subs_instasize(
        upstream_tasks=[
            dims_attribution_instasize_f,
            dims_prodsubs_ios_instasize_f,
        ]
    )
    dims_app_country_pst_installs_f = tasks_sql.dims_app_country_pst_installs(
        upstream_tasks=[
            dims_devices_instasize_f,
            dims_devices_made_f,
            dims_devices_selfiemade_f,
            dims_devices_typeloop_f,
        ]
    )
    # templated sql jobs
    # template_dims_attrabution_f = tasks_sql.template_dims_attrabution.map(
    #     app_name=['selfiemade', 'videomade']
    # )
    template_dims_event_props_f = tasks_sql.template_dims_event_props.map(
            app_name=[
                'instasize_ios', 'instasize_android',
                'made_ios', 'made_android',
                'selfiemade_ios', 'videomade_ios', 'typeloop_ios'
            ]
        )


if __name__ == '__main__':
    from prefect.executors import DaskExecutor

    executor = DaskExecutor(
        cluster_kwargs={
            'processes': True,
            'n_workers': 4,
            'threads_per_worker': 4,
        }
    )
    flow_state = flow.run(executor=executor)
