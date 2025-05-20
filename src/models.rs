use diesel::prelude::*;

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::schema::Page)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Page {
    pub title: String,
    pub url: String,
    pub links: String,
    pub emails: String,
    pub coins: String,
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::schema::WorkUnit)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct WorkUnit {
    pub url: String,
}

