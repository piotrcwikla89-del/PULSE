logo_duze = '''<svg xmlns="http://www.w3.org/2000/svg" width="600" height="150" viewBox="0 0 600 150">
  <defs>
    <linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#00aaff" />
      <stop offset="100%" stop-color="#22dd55" />
    </linearGradient>
    <style> .title { font: bold 56px sans-serif; fill: url(#g); } .subtitle { font: 20px sans-serif; fill: #333; } </style>
  </defs>
  <rect width="600" height="150" fill="#f8fafc" rx="20" />
  <text x="30" y="70" class="title">PULSE</text>
  <text x="30" y="110" class="subtitle">Production Unified Logistics & System Execution</text>
</svg>'''
logo_male = '''<svg xmlns="http://www.w3.org/2000/svg" width="120" height="120" viewBox="0 0 120 120">
  <defs>
    <linearGradient id="gr" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#00aaff" />
      <stop offset="100%" stop-color="#22dd55" />
    </linearGradient>
    <style> .name { font: bold 30px sans-serif; fill: url(#gr); } </style>
  </defs>
  <circle cx="60" cy="60" r="56" fill="#fff" stroke="#ccd9ed" stroke-width="2" />
  <path d="M30,70 L45,50 L60,70 L75,40 L90,70" fill="none" stroke="url(#gr)" stroke-width="8" stroke-linecap="round" />
  <text x="60" y="105" text-anchor="middle" class="name">P</text>
</svg>'''
with open('static/logo_duze.svg', 'w', encoding='utf-8') as f:
    f.write(logo_duze)
with open('static/logo_male.svg', 'w', encoding='utf-8') as f:
    f.write(logo_male)
print('Logo utworzone')