# Phase 2: Live API Integrations & End-to-End Trafficking

> **Context for AI Assistant:** This document serves as a detailed prompt and architectural roadmap for the next major phase of the Disney Ad Ops Lab. Read this document thoroughly to understand the objective before beginning implementation.

## 🎯 The Objective
Transition the current "simulated" EVE trafficking engine (`src/trafficking/engine.py`) into a fully functional automation pipeline. The orchestrator must connect to real advertising APIs (Meta, TikTok, Snapchat, Google Ads), create draft campaigns automatically based on Airtable ticket payloads, push traffic to custom mock landing pages, and handle real-world API responses.

## 📋 The Implementation Plan

When the user initiates this phase, the AI assistant must execute the following steps in order:

### 1. Stand Up Disney Proxy Landing Pages
Before running ads, we need destinations that mimic real Disney conversion funnels to test tracking and URL taxonomy.
*   **Task:** Scaffold a modern, lightweight web application (e.g., Next.js or Vanilla HTML/CSS) to serve as the destination endpoints for our automated ads.
*   **Requirements:**
    *   Create mock pages for at least two products (e.g., `disneyplus-signup-demo.local` and `hulu-promo-demo.local`).
    *   Ensure the pages have visually appealing, premium UI mimicking Disney's design system.
    *   Include placeholder areas where platform tracking pixels (Meta Pixel, TikTok Pixel) can be injected later.
    *   Provide instructions for running this locally or deploying to a free tier (like Vercel).

### 2. Guide User Through Media Developer Account Setup
The AI must provide meticulous, step-by-step instructions for the user to create the necessary developer environments. Provide this one platform at a time to avoid overwhelming the user.
*   **Meta (Facebook/Instagram):** Guide the user to create a Meta Developer Account, set up a System User in Business Manager, generate a never-expiring Access Token, and locate their Ad Account ID.
*   **TikTok / Snapchat / Google:** Outline the process for accessing their respective Marketing APIs, applying for sandbox access, or generating OAuth credentials.

### 3. Build the API Integration Modules
Refactor the Python backend to replace simulated actions with real HTTP requests to the advertising APIs.
*   **Authentication Hub:** Create a secure module to handle OAuth tokens and API keys loaded from the `.env` file.
*   **Platform-Specific Translators:** Build specific handler classes for each API.
    *   Example: `MetaAdsAPIHandler` that takes the generic Disney `TraffickingPayload` and translates it into the specific JSON schema Meta's Graph API requires for Campaign, AdSet, and Ad creation.
*   **Error Handling & Rate Limiting:** Implement robust logic to catch API errors (e.g., invalid targeting, budget too low) and route those errors back to the QA Engine or Alerting Pipeline.

### 4. Connect the End-to-End Pipeline
Tie the live APIs back to the Airtable UI.
*   **Flow:** Airtable Ticket -> Python Orchestrator -> Ad Platform API API -> Success Response -> Python Orchestrator -> Update Airtable Ticket.
*   **Actionable Write-Back:** When an ad platform successfully creates a campaign, the API returns a unique ID (e.g., Meta Campaign ID). The orchestrator must write this exact external ID back to the Airtable `Campaigns` table, closing the loop.

## 🤖 AI Execution Instructions
When reading this prompt to begin the task:
1. Acknowledge this roadmap.
2. Ask the user which task to start with: standing up the landing pages (Step 1) or setting up the first Media API account (Step 2 - Meta is recommended first due to accessible documentation).
3. Do not proceed to Step 3 until credentials for at least one live platform have been secured in the `.env` file.
