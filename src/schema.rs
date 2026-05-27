// @generated automatically by Diesel CLI.

diesel::table! {
    auto_blacklist_event (id) {
        id -> Int4,
        rule_id -> Int4,
        domain -> Text,
        source_page_id -> Nullable<Int4>,
        rule_type -> Text,
        matched_value -> Text,
        evidence -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    auto_blacklist_rule (id) {
        id -> Int4,
        rule_type -> Text,
        value -> Text,
        label -> Text,
        enabled -> Bool,
        created_at -> Text,
    }
}

diesel::table! {
    domain_blacklist (id) {
        id -> Int4,
        domain -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    forum_keyword_rule (id) {
        id -> Int4,
        label -> Text,
        pattern -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    host_http_observation (id) {
        id -> Int4,
        host -> Text,
        scheme -> Text,
        port -> Int4,
        status -> Text,
        http_status_code -> Nullable<Int4>,
        final_url -> Nullable<Text>,
        server_header -> Nullable<Text>,
        powered_by_header -> Nullable<Text>,
        content_type_header -> Nullable<Text>,
        location_header -> Nullable<Text>,
        via_header -> Nullable<Text>,
        alt_svc_header -> Nullable<Text>,
        www_authenticate_header -> Nullable<Text>,
        set_cookie_names -> Nullable<Text>,
        response_headers -> Nullable<Text>,
        header_fingerprint -> Nullable<Text>,
        favicon_url -> Nullable<Text>,
        favicon_hash -> Nullable<Text>,
        last_error -> Nullable<Text>,
        last_attempt_at -> Text,
        last_success_at -> Nullable<Text>,
        created_at -> Text,
        stack_versions -> Nullable<Text>,
        exposed_resources -> Nullable<Text>,
    }
}

diesel::table! {
    host_service_observation (id) {
        id -> Int4,
        host -> Text,
        service -> Text,
        port -> Int4,
        status -> Text,
        banner -> Nullable<Text>,
        banner_fingerprint -> Nullable<Text>,
        last_error -> Nullable<Text>,
        last_attempt_at -> Text,
        last_success_at -> Nullable<Text>,
        created_at -> Text,
    }
}

diesel::table! {
    host_ssh_observation (id) {
        id -> Int4,
        host -> Text,
        port -> Int4,
        status -> Text,
        host_key_algorithm -> Nullable<Text>,
        host_key -> Nullable<Text>,
        host_key_fingerprint -> Nullable<Text>,
        server_banner -> Nullable<Text>,
        last_error -> Nullable<Text>,
        last_attempt_at -> Text,
        last_success_at -> Nullable<Text>,
        created_at -> Text,
    }
}

diesel::table! {
    host_tls_observation (id) {
        id -> Int4,
        host -> Text,
        port -> Int4,
        status -> Text,
        certificate_sha256 -> Nullable<Text>,
        last_error -> Nullable<Text>,
        last_attempt_at -> Text,
        last_success_at -> Nullable<Text>,
        created_at -> Text,
    }
}

diesel::table! {
    intel_lead (id) {
        id -> Int4,
        rule_id -> Text,
        lead_key -> Text,
        title -> Text,
        summary -> Text,
        severity -> Text,
        confidence -> Int4,
        score -> Int4,
        status -> Text,
        primary_entity_type -> Text,
        primary_entity_value -> Text,
        related_entity_type -> Nullable<Text>,
        related_entity_value -> Nullable<Text>,
        first_seen_at -> Text,
        last_seen_at -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    intel_lead_evidence (id) {
        id -> Int4,
        lead_id -> Int4,
        source_type -> Text,
        source_id -> Int4,
        source_key -> Text,
        evidence_text -> Text,
        observed_at -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page (id) {
        id -> Int4,
        title -> Text,
        url -> Text,
        links -> Text,
        emails -> Text,
        coins -> Text,
        language -> Text,
        last_scanned_at -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page_classification (id) {
        id -> Int4,
        page_id -> Int4,
        host -> Text,
        category -> Text,
        confidence -> Text,
        score -> Int4,
        evidence -> Text,
        last_classified_at -> Text,
    }
}

diesel::table! {
    page_crypto (id) {
        id -> Int4,
        page_id -> Int4,
        asset_type -> Text,
        reference -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page_email (id) {
        id -> Int4,
        page_id -> Int4,
        email -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page_keyword_tag (id) {
        id -> Int4,
        page_id -> Int4,
        tag -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page_language_detection (id) {
        id -> Int4,
        page_id -> Int4,
        language_code -> Text,
        language_name -> Text,
        confidence -> Int4,
        source -> Text,
        evidence -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    page_link (id) {
        id -> Int4,
        source_page_id -> Int4,
        target_url -> Text,
        target_host -> Text,
        created_at -> Text,
        source_host -> Text,
    }
}

diesel::table! {
    page_scan (id) {
        id -> Int4,
        page_id -> Int4,
        title -> Text,
        language -> Text,
        scanned_at -> Text,
    }
}

diesel::table! {
    page_scan_crypto (id) {
        id -> Int4,
        scan_id -> Int4,
        asset_type -> Text,
        reference -> Text,
    }
}

diesel::table! {
    page_scan_email (id) {
        id -> Int4,
        scan_id -> Int4,
        email -> Text,
    }
}

diesel::table! {
    page_scan_link (id) {
        id -> Int4,
        scan_id -> Int4,
        target_url -> Text,
        target_host -> Text,
    }
}

diesel::table! {
    page_topic_tag (id) {
        id -> Int4,
        page_id -> Int4,
        topic -> Text,
        score -> Int4,
        confidence -> Text,
        evidence -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    query_log (id) {
        id -> Int4,
        query_name -> Text,
        duration_ms -> Int8,
        executed_at -> Timestamptz,
    }
}

diesel::table! {
    site_profile (id) {
        id -> Int4,
        host -> Text,
        category -> Text,
        confidence -> Text,
        score -> Int4,
        page_count -> Int4,
        evidence -> Text,
        source_page_id -> Nullable<Int4>,
        last_classified_at -> Text,
        created_at -> Text,
        first_found_at -> Text,
        last_scanned_at -> Text,
    }
}

diesel::table! {
    watchlist_item (id) {
        id -> Int4,
        item_type -> Text,
        value -> Text,
        label -> Text,
        enabled -> Bool,
        created_at -> Text,
    }
}

diesel::table! {
    work_unit (id) {
        id -> Int4,
        url -> Text,
        status -> Text,
        retry_count -> Int4,
        next_attempt_at -> Text,
        last_attempt_at -> Nullable<Text>,
        last_error -> Nullable<Text>,
        created_at -> Text,
    }
}

diesel::joinable!(auto_blacklist_event -> auto_blacklist_rule (rule_id));
diesel::joinable!(auto_blacklist_event -> page (source_page_id));
diesel::joinable!(intel_lead_evidence -> intel_lead (lead_id));
diesel::joinable!(page_classification -> page (page_id));
diesel::joinable!(page_crypto -> page (page_id));
diesel::joinable!(page_email -> page (page_id));
diesel::joinable!(page_keyword_tag -> page (page_id));
diesel::joinable!(page_language_detection -> page (page_id));
diesel::joinable!(page_link -> page (source_page_id));
diesel::joinable!(page_scan -> page (page_id));
diesel::joinable!(page_scan_crypto -> page_scan (scan_id));
diesel::joinable!(page_scan_email -> page_scan (scan_id));
diesel::joinable!(page_scan_link -> page_scan (scan_id));
diesel::joinable!(page_topic_tag -> page (page_id));
diesel::joinable!(site_profile -> page (source_page_id));

diesel::allow_tables_to_appear_in_same_query!(
    auto_blacklist_event,
    auto_blacklist_rule,
    domain_blacklist,
    forum_keyword_rule,
    host_http_observation,
    host_service_observation,
    host_ssh_observation,
    host_tls_observation,
    intel_lead,
    intel_lead_evidence,
    page,
    page_classification,
    page_crypto,
    page_email,
    page_keyword_tag,
    page_language_detection,
    page_link,
    page_scan,
    page_scan_crypto,
    page_scan_email,
    page_scan_link,
    page_topic_tag,
    query_log,
    site_profile,
    watchlist_item,
    work_unit,
);
