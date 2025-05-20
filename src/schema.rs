// @generated automatically by Diesel CLI.

diesel::table! {
    Page (id) {
        id -> Integer,
        title -> Text,
        url -> Text,
        links -> Text,
        emails -> Text,
        coins -> Text,
    }
}

diesel::table! {
    WorkUnit (id) {
        id -> Integer,
        url -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    Page,
    WorkUnit,
);
