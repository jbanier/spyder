use anyhow::{Context, Result};
use headless_chrome::{Browser, LaunchOptions};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct ScreenshotConfig {
    pub screenshots_dir: PathBuf,
    pub timeout_seconds: u64,
    pub viewport_width: u32,
    pub viewport_height: u32,
}

impl Default for ScreenshotConfig {
    fn default() -> Self {
        Self {
            screenshots_dir: PathBuf::from("screenshots"),
            timeout_seconds: 30,
            viewport_width: 1920,
            viewport_height: 1080,
        }
    }
}

pub struct ScreenshotCapture {
    config: ScreenshotConfig,
}

impl ScreenshotCapture {
    pub fn new(config: ScreenshotConfig) -> Result<Self> {
        // Create screenshots directory if it doesn't exist
        fs::create_dir_all(&config.screenshots_dir)
            .context("Failed to create screenshots directory")?;

        Ok(Self { config })
    }

    /// Capture a screenshot of the given URL
    /// Returns (screenshot_path, size_in_bytes) on success
    pub fn capture(&self, url: &str, page_id: i32) -> Result<(String, i32)> {
        let launch_options = LaunchOptions {
            headless: true,
            sandbox: false, // Required for I2P/Tor proxied connections
            idle_browser_timeout: Duration::from_secs(self.config.timeout_seconds),
            ..Default::default()
        };

        let browser = Browser::new(launch_options)
            .context("Failed to launch headless browser")?;

        let tab = browser.new_tab()
            .context("Failed to create browser tab")?;

        // Navigate to URL with timeout
        tab.navigate_to(url)
            .context("Failed to navigate to URL")?;

        tab.wait_for_element("body")
            .context("Page body element not found")?;

        // Additional wait for dynamic content
        std::thread::sleep(Duration::from_secs(2));

        // Capture screenshot
        let screenshot_data = tab.capture_screenshot(
            headless_chrome::protocol::cdp::Page::CaptureScreenshotFormatOption::Png,
            None,
            None,
            true,
        ).context("Failed to capture screenshot")?;

        // Generate filename: {page_id}_{timestamp}.png
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("{}_{}.png", page_id, timestamp);
        let filepath = self.config.screenshots_dir.join(&filename);

        // Write screenshot to disk
        fs::write(&filepath, &screenshot_data)
            .context("Failed to write screenshot to disk")?;

        let size_bytes = screenshot_data.len() as i32;
        let relative_path = filepath.to_string_lossy().to_string();

        Ok((relative_path, size_bytes))
    }

    /// Capture screenshot with network-specific timeout
    pub fn capture_with_timeout(&self, url: &str, page_id: i32, timeout_secs: u64) -> Result<(String, i32)> {
        let mut custom_config = self.config.clone();
        custom_config.timeout_seconds = timeout_secs;

        let custom_capture = Self::new(custom_config)?;
        custom_capture.capture(url, page_id)
    }
}

impl Clone for ScreenshotConfig {
    fn clone(&self) -> Self {
        Self {
            screenshots_dir: self.screenshots_dir.clone(),
            timeout_seconds: self.timeout_seconds,
            viewport_width: self.viewport_width,
            viewport_height: self.viewport_height,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_screenshot_config_default() {
        let config = ScreenshotConfig::default();
        assert_eq!(config.timeout_seconds, 30);
        assert_eq!(config.viewport_width, 1920);
        assert_eq!(config.viewport_height, 1080);
    }

    #[test]
    fn test_screenshot_directory_creation() {
        let temp_dir = env::temp_dir().join("spyder_test_screenshots");
        let config = ScreenshotConfig {
            screenshots_dir: temp_dir.clone(),
            ..Default::default()
        };

        let capture = ScreenshotCapture::new(config).expect("Failed to create ScreenshotCapture");
        assert!(temp_dir.exists());

        // Cleanup
        fs::remove_dir_all(temp_dir).ok();
    }
}
