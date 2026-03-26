#!/usr/bin/env python3
"""
Stable Server Startup - Avoids Socket Errors
"""

import os
import sys

def start_server_stable():
    """Start server with stable configuration"""
    
    print("🚀 Starting Enhanced Gate Scanning Server (Stable Mode)")
    print("=" * 60)
    
    # Set stable environment variables
    os.environ['FLASK_ENV'] = 'production'
    os.environ['FLASK_DEBUG'] = 'False'
    os.environ['WERKZEUG_RUN_MAIN'] = 'true'
    
    # Change to the correct directory
    backend_dir = 'c:/Capstone Project/face-gate-access/backend'
    if os.path.exists(backend_dir):
        os.chdir(backend_dir)
        print(f"📁 Changed to: {backend_dir}")
    
    print("\n🔧 Server Configuration:")
    print("   ✅ Debug mode: OFF")
    print("   ✅ Auto-reloader: OFF")
    print("   ✅ Socket optimization: ON")
    print("   ✅ HTTPS enabled: YES")
    
    print("\n🌐 Server will start at:")
    print("   🟢 https://localhost:5444")
    print("   🟢 https://127.0.0.1:5444")
    
    print("\n📱 Enhanced Scanning Pages:")
    print("   🎯 Main Demo: https://localhost:5444/test_enhanced_scanning")
    print("   🎯 Simple Demo: https://localhost:5444/simple_demo")
    print("   🎯 Dashboard: https://localhost:5444/dashboard")
    
    print("\n🔧 Camera Permission Fix:")
    print("   ✅ Multiple camera constraint options")
    print("   ✅ Better error handling")
    print("   ✅ Fallback camera settings")
    print("   ✅ Clear permission messages")
    
    print("\n" + "=" * 60)
    print("🚀 Starting server...")
    
    # Import and run the app
    try:
        from app import app, FLASK_HOST, FLASK_PORT, ssl_context
        
        # Run with stable settings
        app.run(
            host=FLASK_HOST,
            port=FLASK_PORT,
            debug=False,
            use_reloader=False,  # Disable auto-reloader to avoid socket errors
            ssl_context=ssl_context,
        )
        
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        print("\n💡 Try these solutions:")
        print("   1. Make sure port 5444 is not in use")
        print("   2. Check your firewall settings")
        print("   3. Run as administrator if needed")
        print("   4. Try: python stable_server.py")

if __name__ == "__main__":
    start_server_stable()
