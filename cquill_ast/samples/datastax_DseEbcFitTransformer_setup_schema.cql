	TABLE SCHEMA:

	CREATE TABLE cybs_rtd_search_m3.gatling_transaction_followon (
	    request_id text,
	    follow_id text,
	    added_by text,
	    applications set<frozen<application_type>>,
	    apps set<text>,
	    auth_ind text,
	    bin_country text,
	    client_application text,
	    client_application_user text,
	    complete_route text,
	    date_added timestamp,
	    departure_time timestamp,
	    device_fingerprint text,
	    device_id text,
	    device_smart_id text,
	    device_smart_id_confidence int,
	    ip_country text,
	    link_request_id text,
	    mdd_encrypt map<text, text>,
	    mdd_hash map<text, text>,
	    mdd_hmac map<text, text>,
	    merchant_customer_id text,
	    merchant_id text,
	    merchant_reference_no text,
	    partner_original_transaction_id text,
	    passive_profile_decision text,
	    passive_profile_id text,
	    passive_profile_name text,
	    passive_score_result int,
	    payment_amount text,
	    payment_currency text,
	    payment_processor text,
	    payment_solution_type text,
	    payreq_id text,
	    profile_converted_amount text,
	    profile_converted_currency text,
	    profile_decision text,
	    profile_id text,
	    profile_merchant_id text,
	    profile_name text,
	    profile_result_amount text,
	    profile_result_currency text,
	    reseller_id text,
	    score_result int,
	    shipping_method text,
	    solr_query text,
	    subscription_id text,
	    terminal_serial_number text,
	    trans_ref_no set<text>,
	    trans_ref_typecode set<text>,
	    transaction_date timestamp,
	    transaction_reference_typecode set<frozen<transref_typecode_type>>,
	    true_ip_address text,
	    xid text,
	    PRIMARY KEY (request_id, follow_id)
	) WITH CLUSTERING ORDER BY (follow_id ASC)
	    AND bloom_filter_fp_chance = 0.01
	    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
	    AND comment = ''
	    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4'}
	    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
	    AND crc_check_chance = 1.0
	    AND dclocal_read_repair_chance = 0.1
	    AND default_time_to_live = 0
	    AND gc_grace_seconds = 864000
	    AND max_index_interval = 2048
	    AND memtable_flush_period_in_ms = 0
	    AND min_index_interval = 128
	    AND read_repair_chance = 0.0
	    AND speculative_retry = '99PERCENTILE';

	CREATE CUSTOM INDEX cybs_rtd_search_m3_gatling_transaction_followon_solr_query_index ON cybs_rtd_search_m3.gatling_transaction_followon (solr_query)
	USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

	-- FIT --
	CREATE TABLE cybs_rtd_search_m3.gatling_transaction_followon_fit (
	    request_id text,
	    follow_id text,
	    added_by text,
	    applications set<frozen<application_type>>,
	    apps set<text>,
	    auth_ind text,
	    bin_country text,
	    client_application text,
	    client_application_user text,
	    complete_route text,
	    date_added timestamp,
	    departure_time timestamp,
	    device_fingerprint text,
	    device_id text,
	    device_smart_id text,
	    device_smart_id_confidence int,
	    ip_country text,
	    link_request_id text,
	    mdd_encrypt map<text, text>,
	    mdd_hash map<text, text>,
	    mdd_hmac map<text, text>,
	    merchant_customer_id text,
	    merchant_id text,
	    merchant_reference_no text,
	    partner_original_transaction_id text,
	    passive_profile_decision text,
	    passive_profile_id text,
	    passive_profile_name text,
	    passive_score_result int,
	    payment_amount text,
	    payment_currency text,
	    payment_processor text,
	    payment_solution_type text,
	    payreq_id text,
	    profile_converted_amount text,
	    profile_converted_currency text,
	    profile_decision text,
	    profile_id text,
	    profile_merchant_id text,
	    profile_name text,
	    profile_result_amount text,
	    profile_result_currency text,
	    reseller_id text,
	    score_result int,
	    shipping_method text,
	    solr_query text,
	    subscription_id text,
	    terminal_serial_number text,
	    trans_ref_no set<text>,
	    trans_ref_typecode set<text>,
	    transaction_date timestamp,
	    transaction_reference_typecode set<frozen<transref_typecode_type>>,
	    true_ip_address text,
	    xid text,
	    PRIMARY KEY (request_id, follow_id)
	) WITH CLUSTERING ORDER BY (follow_id ASC)
	    AND bloom_filter_fp_chance = 0.01
	    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
	    AND comment = ''
	    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4'}
	    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
	    AND crc_check_chance = 1.0
	    AND dclocal_read_repair_chance = 0.1
	    AND default_time_to_live = 0
	    AND gc_grace_seconds = 864000
	    AND max_index_interval = 2048
	    AND memtable_flush_period_in_ms = 0
	    AND min_index_interval = 128
	    AND read_repair_chance = 0.0
	    AND speculative_retry = '99PERCENTILE';

	CREATE CUSTOM INDEX cybs_rtd_search_m3_gatling_transaction_followon_fit_solr_query_index ON cybs_rtd_search_m3.gatling_transaction_followon_fit (solr_query)
	USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';


	-- FIT --
	CREATE TABLE cybs_rtd_search_m3.gatling_transaction_search_fit (
	    request_id text PRIMARY KEY,
	    account_bin text,
	    account_hash text,
	    account_hmac text,
	    account_suffix text,
	    added_by text,
	    applications set<frozen<application_type>>,
	    apps set<text>,
	    auth_ind text,
	    billing_address1 text,
	    billing_city text,
	    billing_country text,
	    billing_email_encrypt text,
	    billing_email_hash text,
	    billing_email_hmac text,
	    billing_firstname text,
	    billing_lastname text,
	    billing_phone_encrypt text,
	    billing_phone_encryption_keyid text,
	    billing_phone_hash text,
	    billing_phone_hmac text,
	    billing_state text,
	    billing_zipcode text,
	    bin_country text,
	    client_application text,
	    client_application_user text,
	    complete_route text,
	    customer_ip_address text,
	    date_added timestamp,
	    departure_time timestamp,
	    device_fingerprint text,
	    device_id text,
	    device_smart_id text,
	    device_smart_id_confidence int,
	    ingrian_key text,
	    ip_country text,
	    link_request_id text,
	    mdd_encrypt map<text, text>,
	    mdd_hash map<text, text>,
	    mdd_hmac map<text, text>,
	    merchant_customer_id text,
	    merchant_id text,
	    merchant_reference_no text,
	    partner_original_transaction_id text,
	    passive_profile_decision text,
	    passive_profile_id text,
	    passive_profile_name text,
	    passive_score_result int,
	    payment_amount text,
	    payment_currency text,
	    payment_processor text,
	    payment_solution_type text,
	    payment_subtype text,
	    payment_type text,
	    payreq_id text,
	    personal_id_encrypt text,
	    personal_id_hash text,
	    personal_id_hmac text,
	    profile_converted_amount text,
	    profile_converted_currency text,
	    profile_decision text,
	    profile_id text,
	    profile_merchant_id text,
	    profile_name text,
	    profile_result_amount text,
	    profile_result_currency text,
	    reseller_id text,
	    score_result int,
	    shipping_addr_hmac_impl text,
	    shipping_addr_hmac_norm text,
	    shipping_address1 text,
	    shipping_address_hash_implied text,
	    shipping_address_hash_normalized text,
	    shipping_city text,
	    shipping_country text,
	    shipping_firstname text,
	    shipping_lastname text,
	    shipping_method text,
	    shipping_phone_encrypt text,
	    shipping_phone_hash text,
	    shipping_phone_hmac text,
	    shipping_state text,
	    shipping_zipcode text,
	    solr_query text,
	    subscription_id text,
	    terminal_serial_number text,
	    trans_ref_no set<text>,
	    trans_ref_typecode set<text>,
	    transaction_date timestamp,
	    transaction_reference_typecode set<frozen<transref_typecode_type>>,
	    true_ip_address text,
	    xid text
	) WITH bloom_filter_fp_chance = 0.01
	    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
	    AND comment = ''
	    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4'}
	    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
	    AND crc_check_chance = 1.0
	    AND dclocal_read_repair_chance = 0.1
	    AND default_time_to_live = 0
	    AND gc_grace_seconds = 864000
	    AND max_index_interval = 2048
	    AND memtable_flush_period_in_ms = 0
	    AND min_index_interval = 128
	    AND read_repair_chance = 0.0
	    AND speculative_retry = '99PERCENTILE';

	CREATE CUSTOM INDEX cybs_rtd_search_m3_gatling_transaction_search_fit_solr_query_index ON cybs_rtd_search_m3.gatling_transaction_search_fit (solr_query)
	USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';


	*** NORMAL ***

	CREATE TABLE cybs_rtd_search_m3.gatling_transaction_search (
	    request_id text PRIMARY KEY,
	    account_bin text,
	    account_hash text,
	    account_hmac text,
	    account_suffix text,
	    added_by text,
	    applications set<frozen<application_type>>,
	    apps set<text>,
	    auth_ind text,
	    billing_address1 text,
	    billing_city text,
	    billing_country text,
	    billing_email_encrypt text,
	    billing_email_hash text,
	    billing_email_hmac text,
	    billing_firstname text,
	    billing_lastname text,
	    billing_phone_encrypt text,
	    billing_phone_encryption_keyid text,
	    billing_phone_hash text,
	    billing_phone_hmac text,
	    billing_state text,
	    billing_zipcode text,
	    bin_country text,
	    client_application text,
	    client_application_user text,
	    complete_route text,
	    customer_ip_address text,
	    date_added timestamp,
	    departure_time timestamp,
	    device_fingerprint text,
	    device_id text,
	    device_smart_id text,
	    device_smart_id_confidence int,
	    ingrian_key text,
	    ip_country text,
	    link_request_id text,
	    mdd_encrypt map<text, text>,
	    mdd_hash map<text, text>,
	    mdd_hmac map<text, text>,
	    merchant_customer_id text,
	    merchant_id text,
	    merchant_reference_no text,
	    partner_original_transaction_id text,
	    passive_profile_decision text,
	    passive_profile_id text,
	    passive_profile_name text,
	    passive_score_result int,
	    payment_amount text,
	    payment_currency text,
	    payment_processor text,
	    payment_solution_type text,
	    payment_subtype text,
	    payment_type text,
	    payreq_id text,
	    personal_id_encrypt text,
	    personal_id_hash text,
	    personal_id_hmac text,
	    profile_converted_amount text,
	    profile_converted_currency text,
	    profile_decision text,
	    profile_id text,
	    profile_merchant_id text,
	    profile_name text,
	    profile_result_amount text,
	    profile_result_currency text,
	    reseller_id text,
	    score_result int,
	    shipping_addr_hmac_impl text,
	    shipping_addr_hmac_norm text,
	    shipping_address1 text,
	    shipping_address_hash_implied text,
	    shipping_address_hash_normalized text,
	    shipping_city text,
	    shipping_country text,
	    shipping_firstname text,
	    shipping_lastname text,
	    shipping_method text,
	    shipping_phone_encrypt text,
	    shipping_phone_hash text,
	    shipping_phone_hmac text,
	    shipping_state text,
	    shipping_zipcode text,
	    solr_query text,
	    subscription_id text,
	    terminal_serial_number text,
	    trans_ref_no set<text>,
	    trans_ref_typecode set<text>,
	    transaction_date timestamp,
	    transaction_reference_typecode set<frozen<transref_typecode_type>>,
	    true_ip_address text,
	    xid text
	) WITH bloom_filter_fp_chance = 0.01
	    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
	    AND comment = ''
	    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4'}
	    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
	    AND crc_check_chance = 1.0
	    AND dclocal_read_repair_chance = 0.1
	    AND default_time_to_live = 0
	    AND gc_grace_seconds = 864000
	    AND max_index_interval = 2048
	    AND memtable_flush_period_in_ms = 0
	    AND min_index_interval = 128
	    AND read_repair_chance = 0.0
	    AND speculative_retry = '99PERCENTILE';

	CREATE CUSTOM INDEX cybs_rtd_search_m3_gatling_transaction_search_solr_query_index ON cybs_rtd_search_m3.gatling_transaction_search (solr_query)
	USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';


	CREATE TABLE cybs_rtd_search_m3.gatling_transaction_search_cmapi (
	    request_id text,
	    follow_id text,
	    case_mgt_date timestamp,
	    case_mgt_decision text,
	    case_mgt_org text,
	    case_mgt_reviewed_by text,
	    case_owner_orgid text,
	    case_owner_username text,
	    date_modified timestamp,
	    final_decision text,
	    fraudlist_action text,
	    fraudlist_marking_reason text,
	    fraudlist_modified_date timestamp,
	    modified_by text,
	    ownership_modification_date timestamp,
	    priority int,
	    priority_modified_date timestamp,
	    queue_id text,
	    queue_name text,
	    queue_update_date timestamp,
	    sla_action_type text,
	    sla_date_expired timestamp,
	    sla_decision text,
	    sla_exempt text,
	    sla_expired text,
	    sla_modified_date timestamp,
	    sla_move_count int,
	    sla_queue_id text,
	    sla_start_time timestamp,
	    sla_time timestamp,
	    solr_query text,
	    PRIMARY KEY (request_id, follow_id)
	) WITH CLUSTERING ORDER BY (follow_id ASC)
	    AND bloom_filter_fp_chance = 0.01
	    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
	    AND comment = ''
	    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4'}
	    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
	    AND crc_check_chance = 1.0
	    AND dclocal_read_repair_chance = 0.1
	    AND default_time_to_live = 0
	    AND gc_grace_seconds = 864000
	    AND max_index_interval = 2048
	    AND memtable_flush_period_in_ms = 0
	    AND min_index_interval = 128
	    AND read_repair_chance = 0.0
	    AND speculative_retry = '99PERCENTILE';

	CREATE CUSTOM INDEX cybs_rtd_search_m3_gatling_transaction_search_cmapi_solr_query_index ON cybs_rtd_search_m3.gatling_transaction_search_cmapi (solr_query)
	USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

