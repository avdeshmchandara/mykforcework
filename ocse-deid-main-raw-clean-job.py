import hashlib
import logging
import sys
import uuid
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3
import botocore
from pyspark.context import SparkContext
from pyspark.sql.functions import to_date, to_timestamp, col, lit, udf
from pyspark.sql.functions import expr, current_timestamp
from pyspark.sql.functions import regexp_replace, concat, format_string
from pyspark.sql.types import StringType


LOGGER = logging.getLogger(__name__)

'''
Ashok Pavuluri, 6/3/2023
This module contains all reusable Dataframe methods
'''

list_translation_type = ['case_id', 'ssn', 'fein', 'email', 'person_nm',
                         'user_cd', 'phone_num']

client = boto3.client("dynamodb")
pii_local_dict = {}


# Adding audit details to the dataframe
def addAuditInfo(df):
    return df.withColumn("rec_ingested_dt", current_timestamp())


# Get Item from Dynamo DB table
def getItemFromLookupTable(tbl_name, partition_key, sort_key,
                           part_key_val, sort_key_val):
    client = boto3.client("dynamodb")
    response = client.get_item(Key={partition_key: {
        'S': str(part_key_val),
    },
        sort_key: {
        'S': sort_key_val,
    }
    },
        TableName=tbl_name,
    )
    client.close()
    if (response.get('Item')):
        hashed_val = response.get('Item')['hashed_value']['S']
        print("hashed_val:", hashed_val)
        pii_local_dict[str(part_key_val)] = hashed_val
        return hashed_val
    else:
        return ''


def getHashText(pii_value):
    """
        Basic hashing function for a pii_value using random unique salt.
    """
    salt = uuid.uuid4().hex
    return hashlib.sha256(salt.encode() + str(pii_value).encode()).hexdigest()


'''
1.SSN -> SHA-256 HASH with Random Salt
2.FEIN/EIN -> SHA-256 HASH with Random Salt
3.Case ID -> SHA-256 HASH with Random Salt
4.Date of -> Birth Maintain as the decade of birth
=>Exclude fields like 5.First Name, 6.Last Name, 7.Street Address,
8.Email Address, 9.Phone
Add New column name by appending _hash to the source column, then apply hashing
'''


def getFromDict(pii_value):
    return pii_local_dict.get(pii_value)


# Generate new Hashcode or retrived existing one from Dynamo DB lookup tbl
def getOrGenerateHash(col_nm_to_be_hashed, translation_type,
                      pii_value, pii_lkup_table):
    if (pii_value is None or len(pii_value) == 0):
        return ""
    if (getFromDict(pii_value)):
        pre_hashed_code = getFromDict(pii_value)
        print("local read")
    else:
        pre_hashed_code = getItemFromLookupTable(pii_lkup_table, 'pii_value',
                                                 'pii_type', pii_value,
                                                 translation_type)

    print("pre_hashed_code", pre_hashed_code)
    if (pre_hashed_code):  # If case_id already hashed, return older hash code
        return pre_hashed_code
    else:  # Generate new hascode after salting
        new_hash_value = getHashText(pii_value)
        print("new hashvalue is generated:", new_hash_value)
        dic = {'pii_value': pii_value, 'hashed_value': new_hash_value,
               'pii_type': translation_type}

        if dynamoTransactWrite([createDyPutElement(createItems(dic),
                                                   'pii_value',
                                                   pii_lkup_table)]):
            # If TransactionCanceledException conflict occured,
            # then look for new hashed value or generate one again,
            # and update Dynamo table
            pre_hashed_code = (getItemFromLookupTable(
                               pii_lkup_table, 'pii_value', 'pii_type',
                               pii_value, translation_type)
                               )
            if (pre_hashed_code):
                return pre_hashed_code
            else:
                new_hash_value = getHashText(pii_value)
                dic = {'pii_value': pii_value,
                       'hashed_value': new_hash_value,
                       'pii_type': translation_type}

                dynamoTransactWrite(
                    [createDyPutElement(createItems(dic),
                                        'pii_value',
                                        pii_lkup_table)]
                )
        return new_hash_value


# Masking the date ex: 12/12/31 --> 12/12/01 Year and Month remains
# same but day is changed to 01
def getMaskedDate(df, col_nm):
    return (df.withColumn(
        col_nm,
        concat(expr("year("+col_nm+")"),
               format_string("-%02d",
                             expr("month("+col_nm+")")),
               lit("-01"))
    ).withColumn(col_nm, to_date(col(col_nm),
                                 "yyyy-MM-dd"))

    )


def dynamoTransactWrite(items_list):
    print("start dynamoTransactWrite")
    print(items_list)
    explist = []
    try:
        client = boto3.client("dynamodb")
        client.transact_write_items(TransactItems=items_list)
        client.close()
        return False
    # Handle few error codes of TransactionCanceledException
    except client.exceptions.TransactionCanceledException as e1:
        print(e1.response['Error']['Code'])
        return True
    except botocore.exceptions.ClientError as e:
        print(e.response['Error']['Code'])
        explist.append('ConditionalCheckFailedException')
        explist.append('TransactionConflictException')
        if e.response['Error']['Code'] not in explist:
            #  return True
            raise e


# Create Dynamo DB single Item or Record
def createItems(pii_dict):
    temp = {}
    for rec in pii_dict:
        temp.update({rec: {'S': str(pii_dict[rec])}})
    return temp


# Bundle items under Put element.
def createDyPutElement(item, con_exp_col_nm, tbl_nm):
    return {
        "Put": {
            "Item": item,
            'ConditionExpression': 'attribute_not_exists('+con_exp_col_nm+')',
            "TableName": tbl_nm
        }
    }


'''
Code for Glue job starts here.
'''


# Function to process the PII Columns, Translation Types [DOB, SSN, CASE_ID]
def processPIIData(pii_dict_mapping, df_inc_raw):
    print("inside processPIIData")
    if ((not pii_dict_mapping) or (not df_inc_raw.count())):
        return df_inc_raw
    # Register Hashing Funtion
    piiColumnHashUdf = udf(getOrGenerateHash, StringType())
    # Iterate over column name and tranlation type.
    for pii_column_nm, pii_col_type in pii_dict_mapping.items():
        if pii_col_type in list_translation_type:
            df_inc_raw = df_inc_raw.withColumn(pii_column_nm,
                                               piiColumnHashUdf(
                                                   pii_column_nm,
                                                   lit(pii_col_type),
                                                   col(pii_column_nm),
                                                   lit(pii_lkup_table)
                                               ))
        elif pii_col_type == 'dob':
            df_inc_raw = getMaskedDate(df_inc_raw, pii_column_nm)
    print("completed processPIIData")
    return df_inc_raw


# Rename Columns
def renameColsOfDF(rename_cols_dict, df):
    print("inside renameColsOfDF")
    if ((not rename_cols_dict) or (not df.count())):
        return df
    for col_old_nm, col_new_nm in rename_cols_dict.items():
        df = df.withColumnRenamed(col_old_nm, col_new_nm)
    return df


# Drop the columns as per PII guidelines
def excludeColsFromDF(exclude_cols, df):
    print("inside excludeColsFromDF")
    if (not exclude_cols or (not df.count())):
        return df
    return df.drop(*exclude_cols)


def coltransform(fmt, dataset_df, colnm):
    dataset_df = dataset_df.withColumn(colnm,
                                       regexp_replace(col(colnm), fmt, '')
                                       )
    return dataset_df


def main():
    '''
    This dictionary contains - all input dataset properties like
    colNames,PKeys,PIICol etc
    '''
    datasetParamDict = {

        "ofcr_fipscref": {
            "table_name": "ofcr_fipscref",
            "primary_key_col": [
                "st",
                "fips_cnty_cd"
            ],
            "partition_cols": [
                "st"
            ],
            "pii_col": {},
            "exclude_cols": [],
            "s3_target_write_folder": "ofcr/ofcr_fipscref",
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
            "rename_cols": {}
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
            "pii_col": {
                "user_cd": "user_cd"
            },
            "exclude_cols": [],
            "s3_target_write_folder": "ofcr/ofcr_fcrcase",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
            "rename_cols": {}
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
                "ssn": "ssn"
            },
            "exclude_cols": [],
            "s3_target_write_folder": "ofcr/ofcr_famvi",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ofcr_dthinfo": {
            "table_name": "ofcr_dthinfo",
            "primary_key_col": [
                "ssn"
            ],
            "partition_cols": [

            ],
            "pii_col": {
                "ssn": "ssn",
                "dob": "dob",
                "dod": "dob",
                "fnm": "person_nm",
                "mnm": "person_nm",
                "lnm": "person_nm"
            },
            "exclude_cols": [],
            "s3_target_write_folder": "ofcr/ofcr_dthinfo",
            "date_trns_cols": {
                "stus_dt": "yyyy-MM-dd"
            },
            "ts_trns_cols": {},
            "rename_cols": {}
        },

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
            "pii_col": {
                "ssn": "ssn",
                "dob": "dob",
                "dod": "dob",
                "fnm16": "person_nm",
                "mnm16": "person_nm",
                "lnm30": "person_nm"
            },
            "exclude_cols": [
            ],
            "s3_target_write_folder": "ofcr/ofcr_person",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "ofcr_states": {
            "table_name": "ofcr_states",
            "primary_key_col": [
                "st"
            ],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": [
            ],
            "s3_target_write_folder": "ofcr/ofcr_states",
            "date_trns_cols": {},
            "ts_trns_cols": {

            },
            "rename_cols": {}
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
                "ssn": "ssn",
                "oth_ssn": "ssn",
                "oth_fnm16": "person_nm",
                "oth_mnm16": "person_nm",
                "oth_lnm30": "person_nm"
            },
            "exclude_cols": [
            ],
            "s3_target_write_folder": "ofcr/ofcr_other",
            "date_trns_cols": {},
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "pcsenet_txnhdr": {
            "table_name": "pcsenet_txnhdr",
            "primary_key_col": [
                "csenet_idfr"
            ],
            "partition_cols": [
                "fn_cd"
            ],
            "pii_col": {
                "case_idfr": "case_id",
                "oth_case_idfr": "case_id"
            },
            "exclude_cols": [],
            "s3_target_write_folder": "fpls/pcsenet_txnhdr",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
            "rename_cols": {}
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
            "pii_col": {
                "di_case_id": "case_id"
            },
            "exclude_cols": [],
            "s3_target_write_folder": "micrs/fplsrs_fcr_case",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "fplsrs_fcr_prsn_in_case": {
            "table_name": "fplsrs_fcr_prsn_in_case",
            "primary_key_col": [
                "fcr_prsn_in_case_uid"
            ],
            "partition_cols": [
                "st_cd"
            ],
            "pii_col": {
                "di_case_id": "case_id"
            },
            "exclude_cols": [],
            "s3_target_write_folder": "micrs/fplsrs_fcr_prsn_in_case",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
            "rename_cols": {}
        },
        "fplsrs_ndnh_qw": {
            "table_name": "fplsrs_ndnh_qw",
            "primary_key_col": [
                "ndnh_qw_uid"
            ],
            "partition_cols": [],
            "pii_col": {
                "di_ssn": "ssn", "di_fein": "fein"
            },
            "exclude_cols": [
                "src_optnl_addr_id", "src_prmry_addr_id"
            ],
            "s3_target_write_folder": "micrs/fplsrs_ndnh_qw",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
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
            "pii_col": {
                "di_ssn": "ssn"
            },
            "exclude_cols": [],
            "s3_target_write_folder": "micrs/fplsrs_ndnh_ui_claim",
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
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
            "date_trns_cols": {
            },
            "ts_trns_cols": {},
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
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
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
            "ts_trns_cols": {
            },
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
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
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
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_comm_user": {
            "table_name": "comm_comm_user",
            "primary_key_col": [
                "comm_user_uid"
            ],
            "partition_cols": [],
            "pii_col": {
                "emailaddr_txt": "email"
            },
            "exclude_cols": [
                "last_nm_txt",
                "frst_nm_txt"
            ],
            "s3_target_write_folder": "comm/comm_comm_user",
            "date_trns_cols": {},
            "ts_trns_cols": {
            },
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
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_prtcpt": {
            "table_name": "comm_prtcpt",
            "primary_key_col": [
                "prtcpt_uid"
            ],
            "partition_cols": [],
            "pii_col": {
                "emailaddr_txt": "email"
            },
            "exclude_cols": [
                "frst_nm_txt",
                "last_nm_txt"
            ],
            "s3_target_write_folder": "comm/comm_prtcpt",
            "date_trns_cols": {},
            "ts_trns_cols": {
            },
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
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
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
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
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
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
            "rename_cols": {}
        },
        "comm_centl_mailbox_pref": {
            "table_name": "comm_centl_mailbox_pref",
            "primary_key_col": [
                "centl_mailbox_pref_uid"
            ],
            "partition_cols": [],
            "pii_col": {"emailaddr_txt": "email"},
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
            "pii_col": {"ssn": "ssn"},
            "exclude_cols": ["subj"],
            "s3_target_write_folder": "comm/comm_comm",
            "date_trns_cols": {
            },
            "ts_trns_cols": {
            },
            "char_rep_trns": {"ssn": [" ", "-"]},
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
            "exclude_cols": ["msg"],
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
            "exclude_cols": ["atcht_nm"],
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
        "comm_comm_btch_ntfn": {
            "table_name": "comm_comm_btch_ntfn",
            "primary_key_col": [
                "comm_btch_ntfn_uid"
            ],
            "partition_cols": [],
            "pii_col": {"rcver_emailaddr_text": "email"},
            "exclude_cols": [],
            "s3_target_write_folder": "comm/comm_comm_btch_ntfn",
            "date_trns_cols": {
            },
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
        "comm_comm_btch_ntfn_comm": {
            "table_name": "comm_comm_btch_ntfn_comm",
            "primary_key_col": ["comm_btch_ntfn_comm_uid"],
            "partition_cols": [],
            "pii_col": {},
            "exclude_cols": ["sendr_frst_nm_txt", "sendr_last_nm_txt"],
            "s3_target_write_folder": "comm/comm_comm_btch_ntfn_comm",
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
            "exclude_cols": ["auth_rep_sgnr_nm", "auth_rep_sgnr_title"],
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
            "pii_col": {"emailaddr_txt": "email"},
            "exclude_cols": ["frst_nm_txt", "last_nm_txt"],
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
            "pii_col": {"emailaddr_txt": "email", "phnnum_txt": "phone_num"},
            "exclude_cols": [
                "faxnum_txt",
                "frst_nm_txt",
                "last_nm_txt",
                "mnm1_txt",
                "phnext_txt"],
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
            "exclude_cols": ["audt_user_fnm", "audt_user_lnm"],
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
            "pii_col": {"ssn": "ssn"},
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
            "pii_col": {
                "contact_email_txt": "email",
                "contact_phone_num": "phone_num",
                "Intl_phone_num": "phone_num"},
            "exclude_cols": [
                "contact_fax_num",
                "contact_nm",
                "contact_phone_ext_num",
                "Intl_fax_num"],
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
            "exclude_cols": ["Intl_phone_num"],
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
            "s3_target_write_folder": "ptl/ptl_org_prfl_vrfn_cd_ref",
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
            "s3_target_write_folder": "ptl/ptl_org_relationship",
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
            "exclude_cols": [
                "birth_dt",
                "social_security_num",
                "social_security_num_old",
                "user_first_nm",
                "user_last_nm",
                "user_middle_nm"],
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
    args = (getResolvedOptions(sys.argv, ["JOB_NAME", "envname",
            "deid_tbl_name", 'dataset_name', 'source_db', 'target_db'])
            )

    # target_folder = args['target_folder_name']
    # Target folder name to write the delta files
    env_name = args['envname']  # Environment Name
    global pii_lkup_table
    pii_lkup_table = args['deid_tbl_name']  # Dynamo db lookup table name
    dataset_nm = args['dataset_name']  # Dataset name to be processed
    source_db = args['source_db']  # Source database name - Raw Layer
    target_db = args['target_db']  # target_db database name - Clean Layer
    print('source_db: {0} , dataset_nm: {1}  , env_name: {2}, target_db : {3} '
          .format(source_db, dataset_nm, env_name, target_db))

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    print(spark)
    job = Job(glueContext)
    job.init(args["JOB_NAME"]+'_'+dataset_nm, args)

    # Load dataset config dictionary
    dataset_dict_obj = datasetParamDict.get(dataset_nm)
    print(dataset_dict_obj)
    # Target folder name to write the delta files
    target_folder = dataset_dict_obj.get("s3_target_write_folder")
    bucket_name = "s3://ocse-"+env_name+"-da-main-deidentified-data/glue-db/"
    # Complete path to write the delta files
    fld_path = bucket_name + target_folder+"/"
    partition_cols_list = dataset_dict_obj.get("partition_cols")
    print("partition_cols_list", partition_cols_list)

    # 1.Build Dynamic df and also dynamic transformation_ctx based on tbl name
    dataset_ddf = glueContext.create_dynamic_frame.from_catalog(
        database=source_db,
        table_name=dataset_dict_obj.get("table_name"),
        transformation_ctx=dataset_dict_obj.get("table_name")+"_dff",
    )

    print("record count after initial ddf load: ", dataset_ddf.count())

    # Rename columns
    rename_cols_dict = dataset_dict_obj.get("rename_cols")
    print("rename_cols==>", rename_cols_dict)
    dataset_df = renameColsOfDF(rename_cols_dict, dataset_ddf.toDF())

    # 2.Drop the columns from dataframe as per PII guidelines.
    exclude_cols = dataset_dict_obj.get("exclude_cols")
    print(exclude_cols)
    dataset_df = excludeColsFromDF(exclude_cols, dataset_df)

    # transormation of coumns ex: trim, remove hypn
    trns_cols = dataset_dict_obj.get("char_rep_trns", {})
    print(trns_cols)
    for col_nm, trns_format_list in trns_cols.items():
        print("trns_format_list:", trns_format_list)
        print("col_nm:", col_nm)
        for trns_format in trns_format_list:
            dataset_df = coltransform(trns_format, dataset_df, col_nm)
    # dataset_df.show()

    # 3.Convert date format from String to dd-MMM-yy
    dict_date_trans_cols = dataset_dict_obj.get("date_trns_cols", {})
    dict_ts_trans_cols = dataset_dict_obj.get("ts_trns_cols", {})
    dataset_df.printSchema()

    for col_nm, dt_format in dict_date_trans_cols.items():
        print('col_nm : {0} and dt_format:{1}'.format(col_nm, dt_format))
        # format only if column name present in dataframe schema - column list
        if col_nm in dataset_df.columns:
            dataset_df = dataset_df.withColumn(col_nm,
                                               to_date(col(col_nm), dt_format))
    for col_nm, ts_format in dict_ts_trans_cols.items():
        print('col_nm : {0} and ts_format:{1}'.format(col_nm, ts_format))
        # format only if column name present in dataframe schema - column list.
        if col_nm in dataset_df.columns:
            dataset_df = dataset_df.withColumn(col_nm,
                                               to_timestamp(col(col_nm),
                                                            ts_format)
                                               )
    # 4.Add audit information to dataframe
    df_inc_raw = addAuditInfo(dataset_df)
    print("size of inc dataframe", df_inc_raw.count())

    # 5.Process PII column - based on translation type.
    pii_dict_mapping = dataset_dict_obj.get("pii_col")
    inc_df = processPIIData(pii_dict_mapping, df_inc_raw)
    inc_df.printSchema()
    inc_df.show(4)
    print("Incremental workload record count is", inc_df.count())

    # 6.Get primary key and write delta files.
    pk_key_list = (dataset_dict_obj.get("primary_key_col"))
    print("pk_key_list:", pk_key_list)

    # Get DataSink object update update updateBehavior.
    result_sink = glueContext.getSink(
        path=fld_path,
        connection_type="s3",
        updateBehavior="LOG",
        partitionKeys=partition_cols_list,
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx=dataset_dict_obj.get("table_name")+"dff",
        # transformation_ctx="result_sink"
    )
    result_sink.setCatalogInfo(
        catalogDatabase=target_db,
        catalogTableName=dataset_dict_obj.get("table_name")
    )
    # Raw input format conversion from CSV/txt into Parquet.
    result_sink.setFormat("glueparquet")

    if inc_df.count() > 0:
        final_df = DynamicFrame.fromDF(
            inc_df, glueContext, "final_df"
        )

        # Write the dataframe to AWS S3 bucket and,
        # also update the Data Catalogue.
        result_sink.writeFrame(final_df)
    else:
        print("zero records in the file")

    job.commit()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOGGER.error("An unexpected error in main() occurred.", exc_info=e)
        sys.exit(1)
