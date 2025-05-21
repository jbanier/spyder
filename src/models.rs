use diesel::prelude::*;

#[derive(Selectable, Insertable)]
#[diesel(table_name = crate::schema::page)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Page {
    pub title: String,
    pub url: String,
    pub links: String,
    pub emails: String,
    pub coins: String,
}

#[derive(Selectable, Queryable)]
#[diesel(table_name = crate::schema::work_unit)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct WorkUnit {
    pub url: String,
    pub processed: bool,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::work_unit)]
pub struct NewUnit<'a> {
    pub url: &'a String,
}
