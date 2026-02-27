"use client";

import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import Script from "next/script";

export default function TrackingProvider({ children }: { children: React.ReactNode }) {
    const searchParams = useSearchParams();
    const [publisher, setPublisher] = useState<string | null>(null);
    const [campaignId, setCampaignId] = useState<string | null>(null);
    const [sessionEventId, setSessionEventId] = useState<string>("");

    useEffect(() => {
        // 1. Identify where traffic came from
        const utmSource = searchParams.get("utm_source")?.toLowerCase() || null;
        const utmCampaign = searchParams.get("utm_campaign") || null;

        setPublisher(utmSource);
        setCampaignId(utmCampaign);

        // 2. Generate a unique, deduplication Event ID for this session
        // This exact ID must be passed to the backend if this user converts!
        const dedupeId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        setSessionEventId(dedupeId);

        // 3. Fire the custom client-side event depending on publisher
        if (utmSource === "meta") {
            console.log(`🔎 [Meta Pixel] Intiating PageView for Campaign: ${utmCampaign} (EventID: ${dedupeId})`);
            // Note: In real prod, you would call `fbq('track', 'PageView', {}, {eventID: dedupeId})`
        } else if (utmSource === "tiktok") {
            console.log(`🎵 [TikTok Pixel] Intiating PageView for Campaign: ${utmCampaign} (EventID: ${dedupeId})`);
            // Note: In real prod, you would call `ttq.track('PageView', {event_id: dedupeId})`
        }
    }, [searchParams]);

    return (
        <>
            {children}

            {/* Dynamic Pixel Injections Based on UTM Source */}
            {publisher === "meta" && (
                <Script id="meta-pixel" strategy="afterInteractive">
                    {`
            console.log("-> 🟦 Executing strict Meta Base Pixel Injection...");
            !function(f,b,e,v,n,t,s)
            {if(f.fbq)return;n=f.fbq=function(){n.callMethod?
            n.callMethod.apply(n,arguments):n.queue.push(arguments)};
            if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
            n.queue=[];t=b.createElement(e);t.async=!0;
            t.src=v;s=b.getElementsByTagName(e)[0];
            s.parentNode.insertBefore(t,s)}(window, document,'script',
            'https://connect.facebook.net/en_US/fbevents.js');
            fbq('init', 'MOCK_FB_PIXEL_ID');
            fbq('track', 'PageView', {}, { eventID: '${sessionEventId}' });
          `}
                </Script>
            )}

            {publisher === "tiktok" && (
                <Script id="tiktok-pixel" strategy="afterInteractive">
                    {`
            console.log("-> ⬛ Executing strict TikTok Base Pixel Injection...");
            !function (w, d, t) {
              w.TiktokAnalyticsObject=t;var ttq=w[t]=w[t]||[];ttq.methods=["page","track","identify","instances","debug","on","off","once","ready","alias","group","enableCookie","disableCookie"],ttq.setAndDefer=function(t,e){t[e]=function(){t.push([e].concat(Array.prototype.slice.call(arguments,0)))}};for(var i=0;i<ttq.methods.length;i++)ttq.setAndDefer(ttq,ttq.methods[i]);ttq.instance=function(t){for(var e=ttq._i[t]||[],n=0;n<ttq.methods.length;n++)ttq.setAndDefer(e,ttq.methods[n]);return e},ttq.load=function(e,n){var i="https://analytics.tiktok.com/i18n/pixel/events.js";ttq._i=ttq._i||{},ttq._i[e]=[],ttq._i[e]._u=i,ttq._t=ttq._t||{},ttq._t[e]=+new Date,ttq._o=ttq._o||{},ttq._o[e]=n||{};var o=document.createElement("script");o.type="text/javascript",o.async=!0,o.src=i+"?sdkid="+e+"&lib="+t;var a=document.getElementsByTagName("script")[0];a.parentNode.insertBefore(o,a)};
              ttq.load('MOCK_TT_PIXEL_ID');
              ttq.page({ event_id: '${sessionEventId}' });
            }(window, document, 'ttq');
          `}
                </Script>
            )}
        </>
    );
}
