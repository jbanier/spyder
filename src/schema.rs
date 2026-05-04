// @generated automatically by Diesel CLI.

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
    page_link (id) {
        id -> Int4,
        source_page_id -> Int4,
        target_url -> Text,
        target_host -> Text,
        created_at -> Text,
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

diesel::joinable!(page_classification -> page (page_id));
diesel::joinable!(page_crypto -> page (page_id));
diesel::joinable!(page_email -> page (page_id));
diesel::joinable!(page_keyword_tag -> page (page_id));
diesel::joinable!(page_link -> page (source_page_id));
diesel::joinable!(page_scan -> page (page_id));
diesel::joinable!(page_scan_crypto -> page_scan (scan_id));
diesel::joinable!(page_scan_email -> page_scan (scan_id));
diesel::joinable!(page_scan_link -> page_scan (scan_id));
diesel::joinable!(site_profile -> page (source_page_id));

diesel::allow_tables_to_appear_in_same_query!(
    domain_blacklist,
    forum_keyword_rule,
    host_http_observation,
    host_ssh_observation,
    host_tls_observation,
    page,
    page_classification,
    page_crypto,
    page_email,
    page_keyword_tag,
    page_link,
    page_scan,
    page_scan_crypto,
    page_scan_email,
    page_scan_link,
    site_profile,
    work_unit,
);
