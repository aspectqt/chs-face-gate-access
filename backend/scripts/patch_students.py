import os

# Use relative path from backend directory
path = "static/students.js"
if not os.path.exists(path):
    print(f"Error: {path} not found from {os.getcwd()}")
    exit(1)

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Update drawOverlay
new_overlay = """  const drawOverlay = (ctx, width, height, aligned, hasFace) => {
    ctx.clearRect(0, 0, width, height);
    const ring = document.getElementById("faceGuideRing");
    if (ring) {
      if (!hasFace) {
        ring.className = "absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[66%] aspect-square rounded-full border-4 border-slate-400/30 transition-colors duration-300";
      } else if (aligned) {
        ring.className = "absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[66%] aspect-square rounded-full border-4 border-emerald-500 shadow-[0_0_15px_rgba(16,185,129,0.4)] transition-colors duration-300";
      } else {
        ring.className = "absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[66%] aspect-square rounded-full border-4 border-blue-400/60 shadow-[0_0_10px_rgba(96,165,250,0.2)] transition-colors duration-300";
      }
    }
  };"""

# Update startFaceCapture
new_start = """    try {
      const constraints = {
        video: { 
          width: { ideal: 960 }, 
          height: { ideal: 720 }, 
          facingMode: "user"
        },
        audio: false,
      };
      
      state.face.stream = await navigator.mediaDevices.getUserMedia(constraints);
      
      const videoTrack = state.face.stream.getVideoTracks()[0];
      if (videoTrack && typeof videoTrack.applyConstraints === 'function') {
        const capabilities = videoTrack.getCapabilities ? videoTrack.getCapabilities() : {};
        if (capabilities.focusMode && capabilities.focusMode.includes('continuous')) {
          await videoTrack.applyConstraints({ advanced: [{ focusMode: 'continuous' }] });
        }
      }
      
      refs.faceVideo.srcObject = state.face.stream;"""

import re
# Match drawOverlay function
overlay_pattern = r"const drawOverlay = \(ctx, width, height, aligned, hasFace\) => \{.*?\};"
if re.search(overlay_pattern, content, flags=re.DOTALL):
    content = re.sub(overlay_pattern, new_overlay, content, flags=re.DOTALL)
    print("Updated drawOverlay via regex")
else:
    print("Could not find drawOverlay pattern")

# Match startFaceCapture getUserMedia block
start_pattern = r"try \{\s+state\.face\.stream = await navigator\.mediaDevices\.getUserMedia\(\{.*?\}\);\s+refs\.faceVideo\.srcObject = state\.face\.stream;"
if re.search(start_pattern, content, flags=re.DOTALL):
    content = re.sub(start_pattern, new_start, content, flags=re.DOTALL)
    print("Updated startFaceCapture via regex")
else:
    print("Could not find startFaceCapture pattern")

with open(path, "w", encoding="utf-8") as f:
    f.write(content)
print("Finished patching students.js")
