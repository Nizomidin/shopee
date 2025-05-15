import { chromium } from "playwright";
import { promisify } from "util";
import { setTimeout } from "timers";
import dotenv from "dotenv";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

// Load environment variables
dotenv.config();

const sleep = promisify(setTimeout);

const AWS_REGION = "us-west-2";
const AWS_ACCESS_KEY_ID = "AKIAZI2LGOGS7BW4U2X7";
const AWS_SECRET_ACCESS_KEY = "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";

const s3 = new S3Client({
  region: AWS_REGION,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
});

// user agent rotation pool
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
];

// extracts and formats product data from html to json
function extractShopeeProductData(html, shop_id, item_id) {
  try {
    const scriptRegex =
      /<script type="text\/mfe-initial-data" data-module="bW9iaWxlbWFsbC1wcm9kdWN0ZGV0YWlsc3BhZ2U=">(.*?)<\/script>/s;
    const match = html.match(scriptRegex);

    if (!match || !match[1]) {
      console.error("Could not find product data in HTML");
      return null;
    }

    const fullData = JSON.parse(match[1]);
    const productKey = `${shop_id}/${item_id}`;
    const cachedMap =
      fullData?.initialState?.DOMAIN_PDP?.data?.PDP_BFF_DATA?.cachedMap;

    if (!cachedMap || !cachedMap[productKey]) {
      console.error(`No product data found at key ${productKey}`);
      return null;
    }

    return cachedMap[productKey];
  } catch (error) {
    console.error("Error extracting product data:", error);
    return null;
  }
}

// main scraping function with retry logic
async function scrapeShopeeProduct(shop_id, item_id, retryCount = 3) {
  let lastError = null;

  for (let attempt = 0; attempt < retryCount; attempt++) {
    try {
      console.log(
        `Attempt ${
          attempt + 1
        }/${retryCount} for shop_id: ${shop_id}, item_id: ${item_id}`
      );

      if (attempt > 0) {
        const delay = 2000 + Math.random() * 3000;
        console.log(
          `Waiting ${Math.round(delay / 1000)} seconds before retry...`
        );
        await sleep(delay);
      }

      const result = await performScrape(shop_id, item_id);
      if (result) {
        return result;
      }
    } catch (error) {
      lastError = error;
      console.error(`Attempt ${attempt + 1} failed:`, error.message);
    }
  }

  throw new Error(
    `Failed after ${retryCount} attempts: ${
      lastError?.message || "No data returned"
    }`
  );
}

// scraping implementation
async function performScrape(shop_id, item_id) {
  let browser = null;
  let page = null;
  let isScrolling = false;

  try {
    const userAgent =
      USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

    browser = await chromium.launch({
      headless: true,
      slowMo: 20,
      args: [
        "--disable-blink-features=AutomationControlled",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-site-isolation-trials",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-setuid-sandbox",
      ],
    });

    const context = await browser.newContext({
      viewport: { width: 1280, height: 800 },
      userAgent,
      deviceScaleFactor: 1,
      locale: "zh-TW",
      timezoneId: "Asia/Taipei",
      bypassCSP: true,
      ignoreHTTPSErrors: true,
    });

    await context.addCookies([
      {
        name: "",
        value: "1",
        domain: ".shopee.tw",
        path: "/",
        expires: 1234567890,
        httpOnly: false,
        secure: false,
        sameSite: "Lax",
      },
      {
        name: "SPC_SEC_SI",
        value: "",
        domain: "shopee.tw",
        path: "/",
        expires: 1234567890.123456,
        httpOnly: true,
        secure: true,
        sameSite: "Lax",
      },
    ]);

    page = await context.newPage();

    await page.addInitScript(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });

      const originalQuery = window.navigator.permissions.query;
      window.navigator.permissions.query = (parameters) => {
        if (parameters.name === "notifications") {
          return Promise.resolve({ state: "granted" });
        }
        return originalQuery(parameters);
      };

      Object.defineProperty(navigator, "plugins", {
        get: () => [
          {
            0: {
              type: "application/x-google-chrome-pdf",
              suffixes: "pdf",
              description: "Portable Document Format",
              enabledPlugin: Plugin,
            },
            name: "Chrome PDF Plugin",
            filename: "internal-pdf-viewer",
            description: "Portable Document Format",
            length: 1,
          },
        ],
      });
    });

    await page.route(
      "**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,ttf,otf}",
      (route) => route.abort()
    );

    await page.setExtraHTTPHeaders({
      "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
      "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120"',
      "sec-ch-ua-platform": '"Windows"',
      "sec-ch-ua-mobile": "?0",
      Accept: "application/json, text/plain, */*",
      Referer: "https://shopee.tw/",
      "x-requested-with": "XMLHttpRequest",
      "x-shopee-language": "zh-Hant",
    });

    console.log("Initializing session...");
    await page.goto("https://shopee.tw/", { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(200 + Math.random() * 300);

    console.log(
      `Extracting Product Data for shop_id: ${shop_id}, item_id: ${item_id}...`
    );

    const productUrl = `https://shopee.tw/a-i.${shop_id}.${item_id}`;
    await page.goto(productUrl, {
      waitUntil: "domcontentloaded",
      timeout: 20000,
    });

    await page.waitForTimeout(500 + Math.random() * 1000);

    console.log("Capturing Product Content...");
    const html = await page.content();

    console.log("Extracting JSON Product Data...");
    const productData = extractShopeeProductData(html, shop_id, item_id);

    if (productData) {
      console.log("Product data extracted successfully");

      return productData;
    } else {
      console.error("Failed to extract product data");
      return null;
    }
  } catch (error) {
    console.error("Error during scraping:", error);
    throw error;
  } finally {
    isScrolling = false;

    await new Promise((resolve) => setTimeout(resolve, 200));

    if (browser) {
      try {
        await browser.close();
        console.log("Browser closed successfully");
      } catch (closeError) {
        console.error("Error closing browser:", closeError);
      }
    }
  }
}

// Function to remove null values from JSON
function removeNullValues(obj) {
  if (Array.isArray(obj)) {
    return obj
      .map((item) => removeNullValues(item))
      .filter((item) => item !== null);
  } else if (obj !== null && typeof obj === "object") {
    return Object.fromEntries(
      Object.entries(obj)
        .map(([key, value]) => [key, removeNullValues(value)])
        .filter(([_, value]) => value !== null)
    );
  }
  return obj;
}

// Main request handler
const handler = async (event, context) => {
  const { query } = event;

  const temp = query.data.url.split(".");
  const shop_id = parseInt(temp[2], 10);
  const item_id = parseInt(temp[3], 10);

  if (!shop_id || !item_id) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "Missing shop_id or item_id" }),
    };
  }

  try {
    const result = await scrapeShopeeProduct(shop_id, item_id);
    if (!result) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Failed to scrape product data" }),
      };
    }

    const cleanedResult = removeNullValues(result);
    cleanedResult.query = query;

    const timestamp = new Date().toISOString();
    const s3Key = `results/${shop_id}_${item_id}/${timestamp}.json`;

    const putObjectCommand = new PutObjectCommand({
      Bucket: "mrscraper-coupang",
      Key: s3Key,
      Body: JSON.stringify(cleanedResult),
      ContentType: "application/json",
    });

    await s3.send(putObjectCommand);

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Success", s3Key }),
    };
  } catch (error) {
    console.error("Error in handleShopeeRequest:", error);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};

export { handler };
