use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::BigInt;

#[derive(QueryableByName, Debug)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let mut conn = PgConnection::establish(&database_url)?;

    // Total site profiles (all hosts discovered)
    let total_hosts: i64 = sql_query("SELECT COUNT(*) AS count FROM site_profile")
        .get_result::<CountRow>(&mut conn)?
        .count;

    // Classified hosts (hosts with category assigned)
    let classified_hosts: i64 = sql_query(
        "SELECT COUNT(*) AS count FROM site_profile WHERE category != ''"
    )
    .get_result::<CountRow>(&mut conn)?
    .count;

    // Total pages in page table
    let total_pages: i64 = sql_query("SELECT COUNT(*) AS count FROM page")
        .get_result::<CountRow>(&mut conn)?
        .count;

    // Aggregated page count from site_profile
    let aggregated_pages: i64 = sql_query(
        "SELECT COALESCE(SUM(page_count), 0) AS count FROM site_profile"
    )
    .get_result::<CountRow>(&mut conn)?
    .count;

    // Pages with language detection
    let pages_with_language: i64 = sql_query(
        "SELECT COUNT(*) AS count FROM page WHERE language IS NOT NULL AND language != ''"
    )
    .get_result::<CountRow>(&mut conn)?
    .count;

    println!("=== CRAWLER TOTALS ===");
    println!("Total hosts discovered: {}", total_hosts);
    println!("Classified hosts: {} ({:.1}%)",
        classified_hosts,
        (classified_hosts as f64 / total_hosts as f64) * 100.0
    );
    println!();
    println!("Total pages crawled: {}", total_pages);
    println!("Aggregated page count: {}", aggregated_pages);
    println!("Pages with language: {}", pages_with_language);
    println!();
    println!("Pages per host (actual): {:.2}", total_pages as f64 / total_hosts as f64);
    println!("Pages per host (aggregated): {:.2}", aggregated_pages as f64 / total_hosts as f64);

    Ok(())
}
