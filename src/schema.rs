// @generated automatically by Diesel CLI.

diesel::table! {
    page (id) {
        id -> Integer,
        title -> Text,
        url -> Text,
        links -> Text,
        emails -> Text,
        coins -> Text,
    }
}

diesel::table! {
    work_unit (id) {
        id -> Integer,
        url -> Text,
        processed -> Bool,
    }
}

diesel::allow_tables_to_appear_in_same_query!(page, work_unit,);
