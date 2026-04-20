// @generated automatically by Diesel CLI.

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

diesel::joinable!(page_crypto -> page (page_id));
diesel::joinable!(page_email -> page (page_id));
diesel::joinable!(page_link -> page (source_page_id));

diesel::allow_tables_to_appear_in_same_query!(page, page_crypto, page_email, page_link, work_unit,);
