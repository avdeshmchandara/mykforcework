import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
import logging

LOGGER = logging.getLogger(__name__)


def mergeAllDeltaDf(spark, target_loc_path, src_inc_df, *join_keys):
    print("Append incremental load into Delta table")
    if src_inc_df.count() < 1:
        return  # no records in the dataframe
    con_str = ""
    for index, join_key in enumerate(join_keys):
        jkey = "( prev_df."+join_key+"= append_df."+join_key+" )"
        if index == len(join_keys)-1:
            con_str = con_str + jkey
        else:
            con_str = con_str + jkey + " and "
    print(con_str)
    target_delta_df = DeltaTable.forPath(spark, target_loc_path)
    # delta_df.toDF().show(5,True)
    # target_delta_df.vacuum(1)
    # target_delta_df.history().show()
    # Joining the dataframes on primarykey
    result_df = (target_delta_df.alias("prev_df").merge(
                 source=src_inc_df.alias("append_df"),
                 condition=expr(con_str))
                 ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    return result_df


def removeDuplicatesRecords(df, pk):
    cnt = df.dropDuplicates(pk).count()
    colname = 'rec_ingested_dt'
    if 'insrt_ts' in df.columns:
        colname = 'insrt_ts'
    elif 'dms_commit_ts' in df.columns:
        colname = 'dms_commit_ts'
    if (colname in df.columns and df.count() > cnt):
        print("Incoming data has are dulicate records ")
        recrds = df.orderBy(col(colname).desc_nulls_first())
        df_distinct = recrds.dropDuplicates(pk)
        # df_distinct.show()
        print(colname, " is used for dedupe, size:", df_distinct.count())
        return df_distinct
    else:
        print("no duplicates records found in the inc dataframe")
        return df


def main():
    datasetParamDict = {

        "ofcr_person": {
            "table_name": "ofcr_person",
            "primary_key_col": [
                "st",
                "st_case_id",
                "ssn",
                "ssn_seq"
            ],
            "partition_cols": [
                "prtcp_typ",
                "st"
            ],
            "s3_target_write_folder": "ofcr/ofcr_person"
        },
        "ofcr_states": {
            "table_name": "ofcr_states",
            "primary_key_col": [
                "st"
            ],
            "partition_cols": [],
            "pii_col": {},
            "s3_target_write_folder": "ofcr/ofcr_states"
        },
        "ofcr_other": {
            "table_name": "ofcr_other",
            "primary_key_col": [
                "st",
                "st_case_id",
                "ssn",
                "ssn_seq",
                "oth_seq_no"
            ],
            "partition_cols": [
                "st"
            ],
            "pii_col": {
            },
            "s3_target_write_folder": "ofcr/ofcr_other",
            "date_trns_cols": []
        },
        "ofcr_fipscref": {
            "table_name": "ofcr_fipscref",
            "primary_key_col": [
                "st",
                "fips_cnty_cd"
            ],
            "partition_cols": [
                "st"
            ],
            "s3_target_write_folder": "ofcr/ofcr_fipscref",
            "date_trns_cols": []
        },
        "ofcr_fcrcase": {
            "table_name": "ofcr_fcrcase",
            "primary_key_col": [
                "st",
                "st_case_id"
            ],
            "partition_cols": [
                "st",
                "fips_cnty_cd"
            ],
            "s3_target_write_folder": "ofcr/ofcr_fcrcase",
            "date_trns_cols": []
        },
        "ofcr_famvi": {
            "table_name": "ofcr_famvi",
            "primary_key_col": [
                "st",
                "st_case_id",
                "ssn",
                "ssn_seq",
                "fv_dt_estb"
            ],
            "partition_cols": [
                "st"
            ],
            "pii_col": {
            },
            "s3_target_write_folder": "ofcr/ofcr_famvi",
            "date_trns_cols": []
        },
        "ofcr_dthinfo": {
            "table_name": "ofcr_dthinfo",
            "primary_key_col": [
                "ssn"
            ],
            "partition_cols": [
            ],
            "s3_target_write_folder": "ofcr/ofcr_dthinfo",
            "date_trns_cols": []
        },
        "pcsenet_txnhdr": {
            "table_name": "pcsenet_txnhdr",
            "primary_key_col": [
                "csenet_idfr"
            ],
            "partition_cols": [
                "fn_cd"
            ],
            "s3_target_write_folder": "fpls/pcsenet_txnhdr"
        },
        "fplsrs_fcr_case": {
            "table_name": "fplsrs_fcr_case",
            "primary_key_col": [
                "fcr_case_uid"
            ],
            "partition_cols": [
                "st_cd",
                "case_typ_cd"
            ],
            "s3_target_write_folder": "micrs/fplsrs_fcr_case",
            "date_trns_cols": [],
            "rename_cols": {}
        },
        "fplsrs_fcr_prsn_in_case": {
            "table_name": "fplsrs_fcr_prsn_in_case",
            "primary_key_col": [
                "FCR_PRSN_IN_CASE_UID"
            ],
            "partition_cols": [
                "st_cd"
            ],
            "s3_target_write_folder": "micrs/fplsrs_fcr_prsn_in_case",
            "rename_cols": {}
        },
        "fplsrs_ndnh_qw": {
            "table_name": "fplsrs_ndnh_qw",
            "primary_key_col": [
                "ndnh_qw_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "micrs/fplsrs_ndnh_qw",
            "rename_cols": {}
        },
        "fplsrs_ndnh_ui_claim": {
            "table_name": "fplsrs_ndnh_ui_claim",
            "primary_key_col": [
                "ndnh_ui_claim_uid"
            ],
            "partition_cols": [
                "sbmtng_st_cd"
            ],
            "s3_target_write_folder": "micrs/fplsrs_ndnh_ui_claim",
            "rename_cols": {}
        },
        "fplsrs_rs_calendar": {
            "table_name": "fplsrs_rs_calendar",
            "primary_key_col": [
                "PRCSD_DT"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "micrs/fplsrs_rs_calendar",
            "date_trns_cols": {},
            "rename_cols": {}
        },
        "comm_comm_prfl_stus_ref": {
            "table_name": "comm_comm_prfl_stus_ref",
            "primary_key_col": [
                "comm_prfl_stus_cd"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_prfl_stus_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_comm_typ_msg": {
            "table_name": "comm_comm_typ_msg",
            "primary_key_col": [
                "comm_typ_msg_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_typ_msg",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_comm_typ_msg_typ_ref": {
            "table_name": "comm_comm_typ_msg_typ_ref",
            "primary_key_col": [
                "comm_typ_msg_typ_cd"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_typ_msg_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_comm_typ_ref": {
            "table_name": "comm_comm_typ_ref",
            "primary_key_col": [
                "comm_typ_cd"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_comm_user": {
            "table_name": "comm_comm_user",
            "primary_key_col": [
                "comm_user_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_user",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_comm_user_pref_xref": {
            "table_name": "comm_comm_user_pref_xref",
            "primary_key_col": [
                "comm_user_pref_xref_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_user_pref_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_prtcpt": {
            "table_name": "comm_prtcpt",
            "primary_key_col": [
                "prtcpt_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_prtcpt",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_prtcpt_typ_ref": {
            "table_name": "comm_prtcpt_typ_ref",
            "primary_key_col": [
                "prtcpt_typ_cd"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_prtcpt_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_state_ref": {
            "table_name": "comm_state_ref",
            "primary_key_col": [
                "state_cd"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_state_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt": {
            "table_name": "audt",
            "primary_key_col": [
                "audt_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_affiln_comm_rtng_ref": {
            "table_name": "comm_affiln_comm_rtng_ref",
            "primary_key_col": [
                "affiln_comm_rtng_ref_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_affiln_comm_rtng_ref",
            "date_trns_cols": {

            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_affiln_typ_ref": {
            "table_name": "comm_affiln_typ_ref",
            "primary_key_col": [
                "affiln_typ_cd"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_affiln_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_centl_mailbox_pref": {
            "table_name": "comm_centl_mailbox_pref",
            "primary_key_col": [
                "centl_mailbox_pref_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_centl_mailbox_pref",
            "date_trns_cols": {

            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_comm": {
            "table_name": "comm_comm",
            "primary_key_col": [
                "comm_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm",
            "date_trns_cols": {
            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_comm_actn": {
            "table_name": "comm_comm_actn",
            "primary_key_col": [
                "comm_actn_uid"
            ],
            "partition_cols": [],
            "pii_col": {
            },
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_actn",
            "date_trns_cols": {

            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_comm_actn_pref_xref": {
            "table_name": "comm_comm_actn_pref_xref",
            "primary_key_col": [
                "comm_actn_pref_xref_uid"
            ],
            "partition_cols": [],
            "pii_col": {
            },
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_actn_pref_xref",
            "date_trns_cols": {

            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_comm_actn_typ_ref": {
            "table_name": "comm_comm_actn_typ_ref",
            "primary_key_col": [
                "comm_actn_typ_cd"
            ],
            "partition_cols": [],
            "pii_col": {
            },
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_actn_typ_ref",
            "date_trns_cols": {

            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_comm_affiln_typ_ref": {
            "table_name": "comm_comm_affiln_typ_ref",
            "primary_key_col": [
                "comm_affiln_typ_ref_uid"
            ],
            "partition_cols": [],
            "pii_col": {
            },
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_affiln_typ_ref",
            "date_trns_cols": {

            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_comm_app_actvn_ref": {
            "table_name": "comm_comm_app_actvn_ref",
            "primary_key_col": [
                "comm_app_actvn_ref_uid"
            ],
            "partition_cols": [],
            "pii_col": {
            },
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_app_actvn_ref",
            "date_trns_cols": {

            },
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },

        "comm_comm_app_prfl_stus": {
            "table_name": "comm_comm_app_prfl_stus",
            "primary_key_col": [
                "comm_app_prfl_stus_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_app_prfl_stus",
            "date_trns_cols": {},
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_comm_atcht": {
            "table_name": "comm_comm_atcht",
            "primary_key_col": [
                "comm_atcht_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_atcht",
            "date_trns_cols": {},
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_comm_atcht_ctnt_typ_ref": {
            "table_name": "comm_comm_atcht_ctnt_typ_ref",
            "primary_key_col": [
                "comm_atcht_ctnt_typ_cd"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_atcht_ctnt_typ_ref",
            "date_trns_cols": {
            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },

        "comm_comm_pref_comm_typ_msg_xref": {
            "table_name": "comm_comm_pref_comm_typ_msg_xref",
            "primary_key_col": [
                "comm_pref_comm_typ_msg_xref_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_pref_comm_typ_msg_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_comm_pref_org_rlshp": {
            "table_name": "comm_comm_pref_org_rlshp",
            "primary_key_col": [
                "comm_pref_org_rlshp_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_pref_org_rlshp",
            "date_trns_cols": {

            },
            "ts_trns_cols": {

            },
            "rename_cols": {}
        },
        "comm_comm_pref": {
            "table_name": "comm_comm_pref",
            "primary_key_col": [
                "comm_pref_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_pref",
            "date_trns_cols": {},
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_comm_centl_mailbox_pref_xref": {
            "table_name": "comm_comm_centl_mailbox_pref_xref",
            "primary_key_col": [
                "comm_centl_mailbox_pref_xref_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_centl_mailbox_pref_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_comm_btch_ntfn_comm": {
            "table_name": "comm_comm_btch_ntfn_comm",
            "primary_key_col": ["comm_btch_ntfn_comm_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_btch_ntfn_comm",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "comm_comm_btch_ntfn": {
            "table_name": "comm_comm_btch_ntfn",
            "primary_key_col": [
                "comm_btch_ntfn_uid"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_btch_ntfn",
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "prfl_acct": {
            "table_name": "prfl_acct",
            "primary_key_col": ["acct_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_acct",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_acct_org_xref": {
            "table_name": "prfl_acct_org_xref",
            "primary_key_col": ["acct_org_xref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_acct_org_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_affiln_typ_ref": {
            "table_name": "prfl_affiln_typ_ref",
            "primary_key_col": ["affiln_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_affiln_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_cntct_typ_grp_cntct_typ_xref": {
            "table_name": "prfl_cntct_typ_grp_cntct_typ_xref",
            "primary_key_col": ["cntct_typ_grp_cntct_typ_xref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_cntct_typ_grp_cntct_typ_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_cntct_typ_grp_ref": {
            "table_name": "prfl_cntct_typ_grp_ref",
            "primary_key_col": ["affiln_typ_cd", "cntct_typ_grp_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_cntct_typ_grp_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_cntct_typ_ref": {
            "table_name": "prfl_cntct_typ_ref",
            "primary_key_col": ["affiln_typ_cd", "cntct_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_cntct_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_empr_info_stus": {
            "table_name": "prfl_empr_info_stus",
            "primary_key_col": ["empr_info_stus_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_empr_info_stus",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_org_enrlt": {
            "table_name": "prfl_org_enrlt",
            "primary_key_col": ["org_enrlt_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_org_enrlt",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_org_enrlt_stus_ref": {
            "table_name": "prfl_org_enrlt_stus_ref",
            "primary_key_col": ["org_enrlt_stus_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_org_enrlt_stus_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_org_enrlt_typ_ref": {
            "table_name": "prfl_org_enrlt_typ_ref",
            "primary_key_col": ["org_enrlt_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_org_enrlt_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_org_enrlt_user": {
            "table_name": "prfl_org_enrlt_user",
            "primary_key_col": ["org_enrlt_user_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_org_enrlt_user",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prfl_pref": {
            "table_name": "prfl_prfl_pref",
            "primary_key_col": ["prfl_pref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prfl_pref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prfl_pref_qstn_ref": {
            "table_name": "prfl_prfl_pref_qstn_ref",
            "primary_key_col": ["prfl_pref_qstn_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prfl_pref_qstn_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prfl_stus_ref": {
            "table_name": "prfl_prfl_stus_ref",
            "primary_key_col": ["prfl_stus_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prfl_stus_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prsn": {
            "table_name": "prfl_prsn",
            "primary_key_col": ["prsn_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prsn",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prsn_acct_xref": {
            "table_name": "prfl_prsn_acct_xref",
            "primary_key_col": ["prsn_acct_xref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prsn_acct_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prsn_org_cntct_typ_app_xref": {
            "table_name": "prfl_prsn_org_cntct_typ_app_xref",
            "primary_key_col": ["prsn_org_cntct_typ_app_xref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prsn_org_cntct_typ_app_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prsn_org_prsn_updt_audt": {
            "table_name": "prfl_prsn_org_prsn_updt_audt",
            "primary_key_col": ["prsn_org_prsn_updt_audt_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prsn_org_prsn_updt_audt",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_prsn_org_xref": {
            "table_name": "prfl_prsn_org_xref",
            "primary_key_col": ["prsn_org_xref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_prsn_org_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_resp_formt_ref": {
            "table_name": "prfl_resp_formt_ref",
            "primary_key_col": ["resp_formt_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_resp_formt_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "prfl_st_user_id_rlshp": {
            "table_name": "prfl_st_user_id_rlshp",
            "primary_key_col": ["st_user_id_rlshp_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "prfl/prfl_st_user_id_rlshp",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_actn_ref": {
            "table_name": "audt_actn_ref",
            "primary_key_col": ["actn_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_actn_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_application_typ_ref": {
            "table_name": "audt_application_typ_ref",
            "primary_key_col": ["application_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_application_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_audt": {
            "table_name": "audt_audt",
            "primary_key_col": ["audt_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_audt",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_btch_oper_typ_ref": {
            "table_name": "audt_btch_oper_typ_ref",
            "primary_key_col": ["btch_oper_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_btch_oper_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_page_typ_ref": {
            "table_name": "audt_page_typ_ref",
            "primary_key_col": ["page_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_page_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_prc_ref": {
            "table_name": "audt_prc_ref",
            "primary_key_col": ["prc_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_prc_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_state_ref": {
            "table_name": "audt_state_ref",
            "primary_key_col": ["state_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_state_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "audt_tracking": {
            "table_name": "audt_tracking",
            "primary_key_col": ["tracking_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "audt/audt_tracking",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_account_locations": {
            "table_name": "ptl_account_locations",
            "primary_key_col": ["org_prfl_uid", "state_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_account_locations",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_account_status_ref": {
            "table_name": "ptl_account_status_ref",
            "primary_key_col": ["account_status_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_account_status_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_action_typ_ref": {
            "table_name": "ptl_action_typ_ref",
            "primary_key_col": ["action_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_action_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_addr_typ_ref": {
            "table_name": "ptl_addr_typ_ref",
            "primary_key_col": ["addr_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_addr_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_alternative_org_address": {
            "table_name": "ptl_alternative_org_address",
            "primary_key_col": ["alt_addr_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_alternative_org_address",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_alternative_org_address_typ": {
            "table_name": "ptl_alternative_org_address_typ",
            "primary_key_col": ["addr_typ_cd", "alt_addr_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_alternative_org_address_typ",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_app_clsfn_ref": {
            "table_name": "ptl_app_clsfn_ref",
            "primary_key_col": ["app_clsfn_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_app_clsfn_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_contact": {
            "table_name": "ptl_contact",
            "primary_key_col": ["contact_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_contact",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_contact_typ_ref": {
            "table_name": "ptl_contact_typ_ref",
            "primary_key_col": ["contact_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_contact_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_country_ref": {
            "table_name": "ptl_country_ref",
            "primary_key_col": ["country_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_country_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_empr_certn_elgt_stus_ref": {
            "table_name": "ptl_empr_certn_elgt_stus_ref",
            "primary_key_col": ["empr_certn_elgt_stus_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_empr_certn_elgt_stus_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_prfl": {
            "table_name": "ptl_org_prfl",
            "primary_key_col": ["org_prfl_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_org_prfl",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_prfl_addr_typ": {
            "table_name": "ptl_org_prfl_addr_typ",
            "primary_key_col": ["org_prfl_uid", "addr_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_org_prfl_addr_typ",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_prfl_vrfn_cd_ref": {
            "table_name": "ptl_org_prfl_vrfn_cd_ref",
            "primary_key_col": ["prfl_vrfn_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": 
                "ptl/ptl_org_prfl_vrfn_cd_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_relationship": {
            "table_name": "ptl_org_relationship",
            "primary_key_col": [
                "child_org_prfl_uid",
                "parent_org_prfl_uid", 
                "org_relationship_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": 
                "ptl/ptl_org_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_relationship_typ_ref": {
            "table_name": "ptl_org_relationship_typ_ref",
            "primary_key_col": ["org_relationship_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_org_relationship_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_resplt": {
            "table_name": "ptl_org_resplt",
            "primary_key_col": ["org_resplt_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_org_resplt",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_resplt_ref": {
            "table_name": "ptl_org_resplt_ref",
            "primary_key_col": ["org_resplt_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_org_resplt_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_role_app_relationship": {
            "table_name": "ptl_org_role_app_relationship",
            "primary_key_col": ["application_typ_cd", "org_role_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_org_role_app_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_role_typ_ref": {
            "table_name": "ptl_org_role_typ_ref",
            "primary_key_col": ["org_role_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_org_role_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_typ_ref_org_resplt_ref_xref": {
            "table_name": "ptl_org_typ_ref_org_resplt_ref_xref",
            "primary_key_col": ["org_typ_ref_org_resplt_ref_xref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder":
                "ptl/ptl_org_typ_ref_org_resplt_ref_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_org_typ_ref_org_role_typ_ref_xref": {
            "table_name": "ptl_org_typ_ref_org_role_typ_ref_xref",
            "primary_key_col": ["org_typ_ref_org_role_typ_ref_xref_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": 
                "ptl/ptl_org_typ_ref_org_role_typ_ref_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_page_typ_ref": {
            "table_name": "ptl_page_typ_ref",
            "primary_key_col": ["page_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_page_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_prfl_role_app_relationship": {
            "table_name": "ptl_prfl_role_app_relationship",
            "primary_key_col": [
                "org_role_typ_cd",
                "role_typ_cd",
                "application_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_prfl_role_app_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_role_app_relationship": {
            "table_name": "ptl_role_app_relationship",
            "primary_key_col": ["role_typ_cd", "application_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_role_app_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_role_page_relationship": {
            "table_name": "ptl_role_page_relationship",
            "primary_key_col": ["role_typ_cd", "page_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_role_page_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_role_typ_ref": {
            "table_name": "ptl_role_typ_ref",
            "primary_key_col": ["role_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_role_typ_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_state_county_ref": {
            "table_name": "ptl_state_county_ref",
            "primary_key_col": ["state_cd", "county_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_state_county_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_state_role_app_relationship": {
            "table_name": "ptl_state_role_app_relationship",
            "primary_key_col": [
                "state_cd",
                "role_typ_cd",
                "application_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_state_role_app_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_user_account": {
            "table_name": "ptl_user_account",
            "primary_key_col": ["user_account_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_user_account",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_user_affiln_ref": {
            "table_name": "ptl_user_affiln_ref",
            "primary_key_col": ["user_affiln_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_user_affiln_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_user_affiln_subfn_ref": {
            "table_name": "ptl_user_affiln_subfn_ref",
            "primary_key_col": ["user_affiln_subfn_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_user_affiln_subfn_ref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_user_afiln_subfn_role_typ_xref": {
            "table_name": "ptl_user_afiln_subfn_role_typ_xref",
            "primary_key_col": ["user_affiln_subfn_cd", "role_typ_cd"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_user_afiln_subfn_role_typ_xref",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_user_org_relationship": {
            "table_name": "ptl_user_org_relationship",
            "primary_key_col": ["org_prfl_uid", "user_account_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_user_org_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ptl_user_role_app_relationship": {
            "table_name": "ptl_user_role_app_relationship",
            "primary_key_col": [
                "role_typ_cd",
                "application_typ_cd",
                "user_account_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ptl/ptl_user_role_app_relationship",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        }
    }
    # Get Job arguments to be passed to process the raw data
    args = (getResolvedOptions(sys.argv,
            ["JOB_NAME", "envname", 'dataset_name', 'source_db']))
    env_name = args['envname']  # Environment Name
    dataset_nm = args['dataset_name']  # Dataset name to be processed
    source_db = args['source_db']  # Source database name
    print('source_db : {0} , dataset_nm:{1} ,  env_name:{2} '
          .format(source_db, dataset_nm, env_name))

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"]+'_'+dataset_nm, args)

    dataset_dict_obj = datasetParamDict.get(dataset_nm)
    print(dataset_dict_obj)
    # Target folder name to write the delta files
    target_folder = dataset_dict_obj.get("s3_target_write_folder")
    target_s3path = "s3://ocse-"+env_name+"-da-main-curated-data/glue-db/"
    # Complete path to write the delta files
    fld_path = target_s3path+target_folder+"/"
    print("fld_path:", fld_path)
    partition_cols_list = dataset_dict_obj.get("partition_cols")
    print("partition_cols_list", partition_cols_list)

    print("table name :", dataset_dict_obj.get("table_name"))

    dataset_ddf = glueContext.create_dynamic_frame.from_catalog(
        database=source_db,
        table_name=dataset_dict_obj.get("table_name"),
        # transformation_ctx="dataset_ddf"
        transformation_ctx=dataset_dict_obj.get("table_name")+"_dff"
    )

    # 6.Get primary key and write delta files
    pk_key_list = (dataset_dict_obj.get("primary_key_col"))
    print("pk_key_list:", pk_key_list)

    inc_df = dataset_ddf.toDF()
    # inc_df.show(2)
    print("inc_df count", inc_df.count())

    if inc_df.count() > 0:
        inc_df = removeDuplicatesRecords(inc_df, pk_key_list)
    else:
        print("dataset is empty. so no duplicates found.")

    if (DeltaTable.isDeltaTable(spark, fld_path)):
        print("inside the main if for files")
        mergeAllDeltaDf(spark, fld_path, inc_df, *pk_key_list)
    else:
        # First time delta file write
        print("First time delta file writing...")
        additional_options = {
            "path": fld_path,
            "write.parquet.compression-codec": "snappy",
        }
        if inc_df.count() > 0 and len(partition_cols_list) < 1:
            print("Partition columns are not present")
            (inc_df.write.format("delta")
                         .options(**additional_options)
                         .mode("append")
                         .save())
        elif inc_df.count() > 0:
            print("partition cols list:", partition_cols_list)
            (inc_df.write.format("delta")
                         .options(**additional_options)
                         .mode("append")
                         .partitionBy(*partition_cols_list)
                         .save())

    job.commit()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOGGER.error("An unexpected error in main() occurred.", exc_info=e)
        sys.exit(1)