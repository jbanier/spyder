// @generated automatically by Diesel CLI.

diesel::table! {
    page (id) {
        id -> Integer,
        title -> Text,
        url -> Text,
        links -> Text,
        emails -> Text,
        coins -> Text,
        created_at -> Text,
    }
}

diesel::table! {
    work_unit (id) {
        id -> Integer,
        url -> Text,
        status -> Text,
        retry_count -> Integer,
        last_error -> Nullable<Text>,
        created_at -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(page, work_unit,);
