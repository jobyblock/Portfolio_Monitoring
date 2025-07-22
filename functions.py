def create_base_tables(conn,user_name='jobyg' 
):
    from pysnowflake import Session
    sess = Session(
    connection_override_args={
       'autocommit': True,
       'authenticator': 'externalbrowser',
       'account': 'square',
       'database': f'PERSONAL_{user_name.upper()}',
       'user': f'{user_name}@squareup.com'
   }
   
    )
    from datetime import datetime, timedelta
    rt_start_date =  (datetime.now().today()- timedelta(days=2)).strftime("%Y-%m-%d")
    first_start_date=  (datetime.now().today()- timedelta(days=60)).strftime("%Y-%m-%d")

    print('creating attempt base')
    attempt_driver_q = f'''    
create or replace temp table jobys_latest_attempts as (
select a.*, b.p2_overdue_d0_local, b.p2_due_local
from ap_cur_r_frdrisk.curated_fraud_risk_red.unified_feature_datamart_base__{user_name}_dsl3_sv a
left join AP_CUR_RISKBI_G.curated_risk_bi_green.dwm_order_loss_tagging b
on a.order_token = b.order_token
-- where (bp_is_sup = 1 and days_since_first_order_date <= 14 and a.par_region in ('GB', 'US')
where  days_since_first_order_date <= 14 and a.par_region in ('GB', 'US','AU') and a.checkpoint = 'CHECKOUT_CONFIRM'
and a.par_process_date >= '{first_start_date}'
and dedup = 1
);
'''
    conn.execute(attempt_driver_q)

    print('creating decline base')

    rt_driver_decline_q = f'''    
create or replace temp table rt_Declines as (
SELECT order_token, rule_id, par_Region, EVENT_INFO_EVENT_TIME,rule_category
    FROM  AP_CUR_R_FEATSCI.curated_feature_science_red.RAW_C_E_FC_DECISION_RECORD_RULES_RT__{user_name}_DSL3_SV
    where checkpoint in ('CHECKOUT_CONFIRM')
    AND ((is_rejected = 'True' and is_in_treatment ilike 'True'))    
    AND par_process_date >=  '{rt_start_date}'
    AND par_Region in ('AU','GB', 'US')
    and rule_id in (
'au_fraud_online_new_overdue_v3_general'
,'anz_abusive_fraud_online_whitepages_network_score_2024'
,'au_order_velocity_rule_new'
,'ANZ_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'au_fraud_online_recurring_payment_seed_based_linking_rule_v2_migrated'
,'anz_abusive_fraud_online_same_merch_email_streaming_2024'
,'anz_fraud_newconsumer_overdue_v1_replacement'
,'au_online_duplicate_account_identity_type'
,'au_fraud_doordash_decl_history_velocity_rule_v2_migrated'
,'anz_abusive_fraud_online_order_velocity_2024'
,'au_fraud_online_HRM_payback_model_v3_fast'
,'AU_fraud_online_new_user_session_device_linking_eval'
,'anz_fraud_online_newconsumer_device_check_RE_v2_migrated'
,'au_fraud_online_quasi_duplicate_account_written_off'
,'au_fraud_online_card_sharing_new_consumer'
,'au_fraud_online_new_consumer_not_first_order_v2_migrated'
,'anz_fraud_online_hrm_travel'
,'anz_fraud_online_cc_mismatch_email_age_v2_migrated'
,'AU_Online_doordash_high_risk'
,'au_fraud_udf_duplicate_account_freeze'
,'AU_fraud_online_new_user_risky_card_creation_date'
,'au_fraud_online_profile_change_phone_new_user'
,'au_fraud_online_suspicious_group_acount_tier1'
,'anz_fraud_online_velocity_acct_same_merch_email_new_v2_migrated'
,'au_fraud_online_electless_collusion_trend'


--GB SUP
,'GB_fraud_sup_strategy_odv3_general'
,'eu_fraud_online_seed_based_linking_rule_v2_migrated'
,'uk_fraud_online_sup_wpp_phone_check_decline'
,'eu_order_velocity_rule_new'
,'uk_fraud_online_delphi_v3_t14_sup_non_first_order'
,'UK_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'uk_fraud_online_allconsumer_device_check_RE_v2_migrated'
,'gb_fraud_online_quasi_duplicate_account_written_off'
,'eu_fraud_online_wpp_network_score'
,'eu_fraud_online_velocity_order_cnt_same_merch_email_new_streaming_v2_migrated'
,'GB_fraud_online_new_user_session_device_linking'
,'eu_fraud_online_velocity_order_amt_same_merch_email_new_streaming_v2_migrated'
,'eu_fraud_online_duplicate_accounts_tier2_udf_decline'
,'eu_fraud_online_duplicate_accounts_tier2_decline_v2_migrated'
,'eu_fraud_online_fraud_decline_repeated_freeze'
,'GB_fraud_online_new_user_risky_card_bin'
,'gb_fraud_online_profile_change_phone_new_user'
,'gb_fraud_online_quasi_duplicate_account_decline'
,'gb_fraud_online_card_sharing_new_consumer'
,'gb_fraud_online_suspicious_group_acount_tier1'
,'gb_online_payment_reschedule'
,'eu_fraud_online_velocity_acct_same_merch_email_new_streaming_v2_migrated'
,'eu_fraud_online_duplicate_account_collection_seed'
,'eu_fraud_udf_duplicate_account'
,'eu_fraud_online_velocity_acct_same_merch_email_new_v2_migrated'
,'GB_fraud_online_new_user_risky_card_issuing_bank'
,'gb_fraud_online_velocity_new_existing_6h_K_v2_migrated'
,'GB_fraud_online_new_user_risky_email_domain'
,'GB_online_high_order_velocity_rule_v2_migrated'
,'gb_fraud_online_same_merch_velocity'
,'EU_abusive_online_emailage_rule'

--US SUP
,'US_fraud_sup_strategy_odv3_general'
,'us_fraud_online_sup_transaction_model_wpp_info_mismatch_v1'
,'us_fraud_online_seed_based_linking_rule_v2_migrated'
,'NA_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'us_fraud_online_whitepages_general_decline'
,'us_fraud_udf_income_zipcode'
,'cash_credit_abuse_model_rule_v3'
,'us_sup_order_velocity_rule_new'
,'us_fraud_udf_duplicate_account'
,'US_fraud_online_new_user_session_device_linking'
,'us_fraud_online_quasi_duplicate_account_written_off'
,'US_fraud_online_new_user_risky_card_issuing_bank'
,'us_fraud_online_duplicate_accounts_tier2_udf_decline'
,'us_fraud_online_duplicate_accounts_tier2_decline_v2_migrated'
,'cash_credit_abuse_model_rule_realtime_v2'
,'us_fraud_online_fraud_decline_repeated_freeze'
,'us_fraud_online_profile_change_fraud_decline'
,'us_fraud_online_card_sharing_new_consumer'
,'us_fraud_online_velocity_same_merch_email_decline'
,'us_fraud_online_profile_change_phone_new_user'
,'us_fraud_udf_duplicate_account_freeze'
,'us_fraud_online_duplicate_account_collection_seed'
,'US_fraud_online_new_user_risky_card_bin'
,'us_fraud_online_quasi_duplicate_account_decline'
,'US_online_newuser_order_velocity_decline'
,'us_fraud_online_suspicious_group_acount_v2_migrated'
,'US_fraud_online_new_user_risky_email_domain'
,'us_fraud_online_suspicious_group_acount_tier3_v2_migrated'
,'us_fraud_online_velocity_acct_same_merch_email_new_streaming_v2_migrated'
,'us_fraud_online_payment_reschedule'
,'us_fraud_online_new_risky_card_issuing_bank'
,'us_fraud_online_suspicious_group_acount_tier2_v2_migrated'
,'us_fraud_online_duplicate_gmail_accts_new'
,'us_fraud_sup_strategy_whitepages_general_v2'
,'us_fraud_online_velocity_acct_same_merch_email_new_V2'

-- US ONLINE

,'NA_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'US_abusive_online_emailage_rule'
));
'''
    conn.execute(rt_driver_decline_q)

    old_driver_decline_q = f'''create or replace temp table old_declines as (
SELECT order_token, rule_id, par_Region, EVENT_INFO_EVENT_TIME, rule_category
    FROM  AP_CUR_R_FEATSCI.curated_feature_science_red.tBL_RAW_C_E_FC_DECISION_RECORD_RULES__{user_name}_DSL3_SV
    where checkpoint in ('CHECKOUT_CONFIRM')
    AND ((is_rejected = 'True' and is_in_treatment ilike 'True'))    
    AND par_Region in ('AU','GB', 'US')
    and rule_id in (
'au_fraud_online_new_overdue_v3_general'
,'anz_abusive_fraud_online_whitepages_network_score_2024'
,'au_order_velocity_rule_new'
,'ANZ_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'au_fraud_online_recurring_payment_seed_based_linking_rule_v2_migrated'
,'anz_abusive_fraud_online_same_merch_email_streaming_2024'
,'anz_fraud_newconsumer_overdue_v1_replacement'
,'au_online_duplicate_account_identity_type'
,'au_fraud_doordash_decl_history_velocity_rule_v2_migrated'
,'anz_abusive_fraud_online_order_velocity_2024'
,'au_fraud_online_HRM_payback_model_v3_fast'
,'AU_fraud_online_new_user_session_device_linking_eval'
,'anz_fraud_online_newconsumer_device_check_RE_v2_migrated'
,'au_fraud_online_quasi_duplicate_account_written_off'
,'au_fraud_online_card_sharing_new_consumer'
,'au_fraud_online_new_consumer_not_first_order_v2_migrated'
,'anz_fraud_online_hrm_travel'
,'anz_fraud_online_cc_mismatch_email_age_v2_migrated'
,'AU_Online_doordash_high_risk'
,'au_fraud_udf_duplicate_account_freeze'
,'AU_fraud_online_new_user_risky_card_creation_date'
,'au_fraud_online_profile_change_phone_new_user'
,'au_fraud_online_suspicious_group_acount_tier1'
,'anz_fraud_online_velocity_acct_same_merch_email_new_v2_migrated'
,'au_fraud_online_electless_collusion_trend'

--GB SUP
,'GB_fraud_sup_strategy_odv3_general'
,'eu_fraud_online_seed_based_linking_rule_v2_migrated'
,'uk_fraud_online_sup_wpp_phone_check_decline'
,'eu_order_velocity_rule_new'
,'uk_fraud_online_delphi_v3_t14_sup_non_first_order'
,'UK_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'uk_fraud_online_allconsumer_device_check_RE_v2_migrated'
,'gb_fraud_online_quasi_duplicate_account_written_off'
,'eu_fraud_online_wpp_network_score'
,'eu_fraud_online_velocity_order_cnt_same_merch_email_new_streaming_v2_migrated'
,'GB_fraud_online_new_user_session_device_linking'
,'eu_fraud_online_velocity_order_amt_same_merch_email_new_streaming_v2_migrated'
,'eu_fraud_online_duplicate_accounts_tier2_udf_decline'
,'eu_fraud_online_duplicate_accounts_tier2_decline_v2_migrated'
,'eu_fraud_online_fraud_decline_repeated_freeze'
,'GB_fraud_online_new_user_risky_card_bin'
,'gb_fraud_online_profile_change_phone_new_user'
,'gb_fraud_online_quasi_duplicate_account_decline'
,'gb_fraud_online_card_sharing_new_consumer'
,'gb_fraud_online_suspicious_group_acount_tier1'
,'gb_online_payment_reschedule'
,'eu_fraud_online_velocity_acct_same_merch_email_new_streaming_v2_migrated'
,'eu_fraud_online_duplicate_account_collection_seed'
,'eu_fraud_udf_duplicate_account'
,'eu_fraud_online_velocity_acct_same_merch_email_new_v2_migrated'
,'GB_fraud_online_new_user_risky_card_issuing_bank'
,'gb_fraud_online_velocity_new_existing_6h_K_v2_migrated'
,'GB_fraud_online_new_user_risky_email_domain'
,'GB_online_high_order_velocity_rule_v2_migrated'
,'gb_fraud_online_same_merch_velocity'
,'EU_abusive_online_emailage_rule'

--US SUP
,'US_fraud_sup_strategy_odv3_general'
,'us_fraud_online_sup_transaction_model_wpp_info_mismatch_v1'
,'us_fraud_online_seed_based_linking_rule_v2_migrated'
,'NA_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'us_fraud_online_whitepages_general_decline'
,'us_fraud_udf_income_zipcode'
,'cash_credit_abuse_model_rule_v3'
,'us_sup_order_velocity_rule_new'
,'us_fraud_udf_duplicate_account'
,'US_fraud_online_new_user_session_device_linking'
,'us_fraud_online_quasi_duplicate_account_written_off'
,'US_fraud_online_new_user_risky_card_issuing_bank'
,'us_fraud_online_duplicate_accounts_tier2_udf_decline'
,'us_fraud_online_duplicate_accounts_tier2_decline_v2_migrated'
,'cash_credit_abuse_model_rule_realtime_v2'
,'us_fraud_online_fraud_decline_repeated_freeze'
,'us_fraud_online_profile_change_fraud_decline'
,'us_fraud_online_card_sharing_new_consumer'
,'us_fraud_online_velocity_same_merch_email_decline'
,'us_fraud_online_profile_change_phone_new_user'
,'us_fraud_udf_duplicate_account_freeze'
,'us_fraud_online_duplicate_account_collection_seed'
,'US_fraud_online_new_user_risky_card_bin'
,'us_fraud_online_quasi_duplicate_account_decline'
,'US_online_newuser_order_velocity_decline'
,'us_fraud_online_suspicious_group_acount_v2_migrated'
,'US_fraud_online_new_user_risky_email_domain'
,'us_fraud_online_suspicious_group_acount_tier3_v2_migrated'
,'us_fraud_online_velocity_acct_same_merch_email_new_streaming_v2_migrated'
,'us_fraud_online_payment_reschedule'
,'us_fraud_online_new_risky_card_issuing_bank'
,'us_fraud_online_suspicious_group_acount_tier2_v2_migrated'
,'us_fraud_online_duplicate_gmail_accts_new'
,'us_fraud_sup_strategy_whitepages_general_v2'
,'us_fraud_online_velocity_acct_same_merch_email_new_V2'

--US ONLINE
,'NA_FRAUD_TMX_ONLINE_NEW_USERS_V1'
,'US_abusive_online_emailage_rule'
)

    AND par_process_date >=  '{first_start_date}'
    and par_process_date <= '{rt_start_date}');'''

    conn.execute(old_driver_decline_q)

    conn.execute('''create or replace temp table full_declines as (
select * from rt_declines union select * from old_Declines
);''')
    
    print('working on unique declines')

    conn.execute(f'''create or replace temp table unique_declines_rt as (
SELECT order_token, count(distinct(rule_id)) as rule_ct, 
    FROM  AP_CUR_R_FEATSCI.curated_feature_science_red.RAW_C_E_FC_DECISION_RECORD_RULES_RT__{user_name}_DSL3_SV
    where checkpoint in ('CHECKOUT_CONFIRM')
    AND ((is_rejected = 'True' and is_in_treatment ilike 'True'))    
    AND par_Region in ('GB','US','AU')
    AND par_process_date >=  '{first_start_date}'
    and par_process_date <= '{rt_start_date}'
    group by 1
    having  count(distinct(rule_id)) = 1
);''')
    
    conn.execute(f'''
                 create or replace temp table unique_declines_old as (
SELECT order_token, count(distinct(rule_id)) as rule_ct, 
    FROM  AP_CUR_R_FEATSCI.curated_feature_science_red.TBL_RAW_C_E_FC_DECISION_RECORD_RULES__{user_name}_DSL3_SV
    where checkpoint in ('CHECKOUT_CONFIRM')
    AND ((is_rejected = 'True' and is_in_treatment ilike 'True'))    
    AND par_process_date >=  '{first_start_date}'
    and par_process_date <= '{rt_start_date}'
    AND par_Region in ('GB','US','AU')
    group by 1
    having  count(distinct(rule_id)) = 1
); ''')
    
    conn.execute('''create or replace temp table unique_declines as (
select * from unique_declines_rt union select * from unique_declines_old
);''')
    
    print('working on trust')
    conn.execute(f'''create or replace temp table trust_rt as (
SELECT order_token, count(distinct(rule_id)) as rule_ct, 
    FROM  AP_CUR_R_FEATSCI.curated_feature_science_red.RAW_C_E_FC_DECISION_RECORD_RULES_RT__{user_name}_DSL3_SV
    where checkpoint in ('CHECKOUT_CONFIRM')
    AND actions ilike '%trust%'
    AND par_Region in ('GB','US','AU')
    AND par_process_date >=  '{first_start_date}'
    and par_process_date <= '{rt_start_date}'
    group by 1
);''')
    conn.execute(f'''create or replace temp table trust_old as (
SELECT order_token, count(distinct(rule_id)) as rule_ct, 
    FROM  AP_CUR_R_FEATSCI.curated_feature_science_red.TBL_RAW_C_E_FC_DECISION_RECORD_RULES__{user_name}_DSL3_SV
    where checkpoint in ('CHECKOUT_CONFIRM')
    AND actions ilike '%trust%'
    AND par_process_date >=  '{first_start_date}'
    and par_process_date <= '{rt_start_date}'
    AND par_region in ('GB','US','AU')
    group by 1
);''')

    conn.execute('''create or replace temp table trust as (
select * from trust_rt union select * from trust_old
);''')

import pandas as pd

def summarize_and_compare_metrics_v2(df, country, bp_is_sup, p2d0_lag_days=15):
    df = df[(df['bp_is_sup'] == bp_is_sup) & (df['par_region'] == f'{country}')]
    df['par_process_date'] = pd.to_datetime(df['par_process_date'])
    latest_date = df['par_process_date'].max()

    def get_week_bounds(reference_date):
        end = reference_date - pd.Timedelta(days=reference_date.weekday() - 6)
        start = end - pd.Timedelta(days=6)
        prev_end = start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=6)
        return (start, end, prev_start, prev_end)

    def pct_change(current, previous):
        if pd.isna(current) or pd.isna(previous) or previous == 0:
            return None
        return (current - previous) / previous * 100

    def format_pct(value):
        return f"{value:.1f}%" if value is not None else "N/A"

    def format_val(value, is_pct=False):
        if value is None:
            return "N/A"
        return f"{value:.2%}" if is_pct else f"${value:,.0f}"

    def weighted_avg(df_subset):
        attempts = df_subset['attempt_ct'].sum()
        return (df_subset['decline_rt'] * df_subset['attempt_ct']).sum() / attempts if attempts else None

    ### 1. Decline Rate
    decline_ref = latest_date
    cur_start, cur_end, prev_start, prev_end = get_week_bounds(decline_ref)

    df_today = df[df['par_process_date'] == decline_ref]
    df_yesterday = df[df['par_process_date'] == decline_ref - pd.Timedelta(days=1)]
    df_wow = df[(df['par_process_date'] >= cur_start) & (df['par_process_date'] <= cur_end)]
    df_prev_wow = df[(df['par_process_date'] >= prev_start) & (df['par_process_date'] <= prev_end)]

    decline_today = weighted_avg(df_today)
    decline_yesterday = weighted_avg(df_yesterday)
    decline_wow = weighted_avg(df_wow)
    decline_prev_wow = weighted_avg(df_prev_wow)

    ### 2. Approved Amount
    approved_ref = latest_date - pd.Timedelta(days=2)
    cur_start, cur_end, prev_start, prev_end = get_week_bounds(approved_ref)

    approved_today = df[df['par_process_date'] == approved_ref]['approved_amt'].sum()
    approved_yesterday = df[df['par_process_date'] == approved_ref - pd.Timedelta(days=1)]['approved_amt'].sum()
    # print(cur_start,cur_end)
    approved_wow = df[(df['par_process_date'] >= cur_start) & (df['par_process_date'] <= cur_end)]['approved_amt'].sum()
    approved_prev_wow = df[(df['par_process_date'] >= prev_start) & (df['par_process_date'] <= prev_end)]['approved_amt'].sum()

    ### 3. P2D0 Rate
    p2d0_ref = latest_date - pd.Timedelta(days=p2d0_lag_days)
    cur_start, cur_end, prev_start, prev_end = get_week_bounds(p2d0_ref)

    p2d0_today = df[df['par_process_date'] == p2d0_ref]['portfolio_p2_do'].mean()
    p2d0_yesterday = df[df['par_process_date'] == p2d0_ref - pd.Timedelta(days=1)]['portfolio_p2_do'].mean()
    p2d0_wow = df[(df['par_process_date'] >= cur_start) & (df['par_process_date'] <= cur_end)]['portfolio_p2_do'].mean()
    p2d0_prev_wow = df[(df['par_process_date'] >= prev_start) & (df['par_process_date'] <= prev_end)]['portfolio_p2_do'].mean()

    ### Final summary
    lines = []

    # Decline rate summary
    dod_decline = pct_change(decline_today, decline_yesterday)
    wow_decline = pct_change(decline_wow, decline_prev_wow)
    if bp_is_sup:
        lines.append(f'performance for {country} SUP is:')
    else:
        lines.append(f'performance for {country} Online is:')


    lines.append(f"📉 *Decline Rate* (as of {decline_ref.date()}): {format_val(decline_today, is_pct=True)} "
                 f"(DoD: {format_pct(dod_decline)}, WoW: {format_pct(wow_decline)})")

    # Approved amount summary
    dod_approved = pct_change(approved_today, approved_yesterday)
    wow_approved = pct_change(approved_wow, approved_prev_wow)
    lines.append(f"💰 *Approved Amount* (as of {approved_ref.date()}): {format_val(approved_today)} "
                 f"(DoD: {format_pct(dod_approved)}, WoW: {format_pct(wow_approved)})")

    # P2D0 rate summary
    dod_p2d0 = pct_change(p2d0_today, p2d0_yesterday)
    wow_p2d0 = pct_change(p2d0_wow, p2d0_prev_wow)
    lines.append(f"⏱️ *P2D0 Rate* (as of {p2d0_ref.date()}): {format_val(p2d0_today, is_pct=True)} "
                 f"(DoD: {format_pct(dod_p2d0)}, WoW: {format_pct(wow_p2d0)})")

    output = '\n'.join(lines)
    print(output)    

def create_graphs(df, metric,bp_is_sup):

    import seaborn as sns
    import matplotlib.pyplot as plt

# --- Step 1: Filter by bp_is_sup values ---
    df_sup = df[df['bp_is_sup'] == bp_is_sup]

# --- Step 2: Group and aggregate the data by date and region ---
    df_sup_agg = df_sup.groupby(['par_process_date', 'par_region'])[metric].mean().reset_index()

# --- Step 3: Plot for bp_is_sup = 1 ---
    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=df_sup_agg,
        x='par_process_date',
        y=metric,
        hue='par_region',
        marker='o'
)
    plt.title(f'{metric} Over Time by Region (bp_is_sup = {bp_is_sup})')
    plt.xlabel('Processing Date')
    plt.ylabel(f'{metric}')
    plt.legend(title='Region')
    plt.ylim(0, 1)

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()