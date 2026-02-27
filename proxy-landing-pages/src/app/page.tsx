import Image from "next/image";
import { Button } from "@/components/ui/button";

export default function Home() {
  return (
    <div className="min-h-screen bg-[#040714] text-white selection:bg-blue-500">
      {/* Navbar Placeholder */}
      <nav className="fixed w-full z-50 bg-[#040714] px-8 py-4 flex items-center justify-between border-b border-white/10">
        <div className="flex items-center gap-8">
          <div className="text-2xl font-bold tracking-tighter cursor-pointer text-white">Disney+</div>
          <div className="hidden md:flex gap-6 text-sm font-semibold tracking-wider text-gray-300">
            <a href="#" className="hover:text-white transition-colors">ORIGINALS</a>
            <a href="#" className="hover:text-white transition-colors">MOVIES</a>
            <a href="#" className="hover:text-white transition-colors">SERIES</a>
          </div>
        </div>
        <Button variant="outline" className="bg-transparent text-white border-white hover:bg-white hover:text-black transition-colors rounded-sm uppercase tracking-widest text-sm font-semibold h-10 px-6">
          Log In
        </Button>
      </nav>

      {/* Hero Section */}
      <main className="relative pt-32 pb-20 px-4 md:px-8 max-w-7xl mx-auto flex flex-col md:flex-row items-center justify-between min-h-[85vh]">
        {/* Background Gradient Effect - Purely CSS */}
        <div className="absolute inset-0 z-0 bg-gradient-radial from-blue-900/20 via-[#040714]/80 to-[#040714] pointer-events-none" />

        <div className="z-10 w-full md:w-1/2 space-y-8 animate-fade-in-up">
          <div className="space-y-4">
            <h1 className="text-5xl md:text-7xl font-bold tracking-tight text-white leading-tight">
              Endless Entertainment. <br/>
              <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-indigo-500">
                Anywhere.
              </span>
            </h1>
            <p className="text-xl text-gray-400 font-medium max-w-lg leading-relaxed">
              Stream the greatest stories from Disney, Pixar, Marvel, Star Wars, and Nat Geo.
            </p>
          </div>

          <div className="space-y-4">
            <div className="flex flex-col sm:flex-row gap-4">
              <Button className="bg-blue-600 hover:bg-blue-500 text-white border-none text-lg font-bold uppercase tracking-widest h-16 px-12 rounded flex-1 transition-all hover:scale-105 shadow-[0_0_20px_rgba(37,99,235,0.4)]">
                Sign Up Now
              </Button>
            </div>
            <p className="text-xs text-gray-500 tracking-wide text-center sm:text-left">
              $7.99/month. Cancel anytime. Terms apply.
            </p>
          </div>
          
          {/* Tracking Pixel Placeholder Notice (For the Developer) */}
          <div className="mt-12 p-4 bg-gray-900/50 border border-blue-500/30 rounded-lg max-w-md">
            <div className="flex items-center gap-2 text-sm text-blue-400 font-mono mb-2">
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="m18 16 4-4-4-4"/><path d="m6 8-4 4 4 4"/><path d="m14.5 4-5 16"/></svg>
              QA Notice
            </div>
            <p className="text-xs text-gray-400">
              This is a sandbox endpoint. Add ?utm_campaign=xyz to test taxonomy tracking. Base Pixel code injections will go directly into the Next.js `&lt;head&gt;`.
            </p>
          </div>
        </div>
        
        {/* Abstract Floating Element Instead of Image to ensure it always renders */}
        <div className="z-10 w-full md:w-1/2 mt-16 md:mt-0 flex justify-center perspective-[1000px]">
          <div className="relative w-72 h-96 md:w-96 md:h-[32rem] bg-gradient-to-tr from-blue-600/20 via-indigo-900/40 to-purple-800/20 rounded-2xl border border-white/10 backdrop-blur-xl flex items-center justify-center transform hover:rotate-y-12 transition-transform duration-700 shadow-2xl">
            <div className="absolute inset-0 bg-blue-500/10 blur-[100px] rounded-full mix-blend-screen" />
            <div className="text-4xl font-black text-white/50 tracking-tighter rotate-[-15deg] select-none">
              MOCK PREVIEW
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
