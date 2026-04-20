// @generated automatically by Diesel CLI.

diesel::table! {
    domain_blacklist (id) {
        id -> Integer,
        domain -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page (id) {
        id -> Integer,
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
        id -> Integer,
        page_id -> Integer,
        host -> Text,
        category -> Text,
        confidence -> Text,
        score -> Integer,
        evidence -> Text,
        last_classified_at -> Text,
    }
}

diesel::table! {
    page_crypto (id) {
        id -> Integer,
        page_id -> Integer,
        asset_type -> Text,
        reference -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page_email (id) {
        id -> Integer,
        page_id -> Integer,
        email -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page_link (id) {
        id -> Integer,
        source_page_id -> Integer,
        target_url -> Text,
        target_host -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    page_scan (id) {
        id -> Integer,
        page_id -> Integer,
        title -> Text,
        language -> Text,
        scanned_at -> Text,
    }
}

diesel::table! {
    page_scan_crypto (id) {
        id -> Integer,
        scan_id -> Integer,
        asset_type -> Text,
        reference -> Text,
    }
}

diesel::table! {
    page_scan_email (id) {
        id -> Integer,
        scan_id -> Integer,
        email -> Text,
    }
}

diesel::table! {
    page_scan_link (id) {
        id -> Integer,
        scan_id -> Integer,
        target_url -> Text,
        target_host -> Text,
    }
}

diesel::table! {
    site_profile (id) {
        id -> Integer,
        host -> Text,
        category -> Text,
        confidence -> Text,
        score -> Integer,
        page_count -> Integer,
        evidence -> Text,
        source_page_id -> Nullable<Integer>,
        last_classified_at -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    work_unit (id) {
        id -> Integer,
        url -> Text,
        status -> Text,
        retry_count -> Integer,
        next_attempt_at -> Text,
        last_attempt_at -> Nullable<Text>,
        last_error -> Nullable<Text>,
        created_at -> Text,
    }
}

diesel::joinable!(page_classification -> page (page_id));
diesel::joinable!(page_crypto -> page (page_id));
diesel::joinable!(page_email -> page (page_id));
diesel::joinable!(page_link -> page (source_page_id));
diesel::joinable!(page_scan -> page (page_id));
diesel::joinable!(page_scan_crypto -> page_scan (scan_id));
diesel::joinable!(page_scan_email -> page_scan (scan_id));
diesel::joinable!(page_scan_link -> page_scan (scan_id));

diesel::allow_tables_to_appear_in_same_query!(
    domain_blacklist,
    page,
    page_classification,
    page_crypto,
    page_email,
    page_link,
    page_scan,
    page_scan_crypto,
    page_scan_email,
    page_scan_link,
    site_profile,
    work_unit,
);
