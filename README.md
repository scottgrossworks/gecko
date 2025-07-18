
Gekko's Birthday Landing Page & Newsletter System

Scott Gross
scottgross.works/gekko
scottgrossworks@gmail.com

==========================================================================


Overview


Gekko's Birthday is a responsive, movie-inspired landing page and newsletter platform that delivers curated business, technology, and culture news in the spirit of the iconic Wall Street scene. The project features a visually striking, mobile-friendly UI and an automated backend for newsletter distribution.
--------------------------------------------------------------------------


Features


Dynamic Landing Page
Movie-inspired header with current PST time and date
Responsive layout, double-bordered "GEKKO'S BIRTHDAY" banner, and slideshow
Newsletter pitch with Bud Fox/Wall Street references
AWS-integrated newsletter subscription form
Links to MBA Library, Whitepapers, and FAQ
FAQ section for user guidance
Subtle fair use disclaimer footer


--------------------------------------------------------------------------


Newsletter Distribution


Bulk email to all subscribers
Single email preview to a specific recipient
Direct HTML view in browser


--------------------------------------------------------------------------


GECKO/
├── index.html                 # Main landing page
├── css/
│   └── gecko_landing.css      # Main styles (responsive, mobile-first)
├── img/
│   ├── SGW_favicon.ico        # Logo/favicon used in header and browser tab
│   └── ...                    # Slideshow and other images
├── js/
│   └── gecko_landing.js       # Handles time/date, slideshow, and subscribe form
├── aws/
│   ├── gecko_publisher.py     # Newsletter publishing and subscriber management
│   ├── gecko_preview.py       # Newsletter preview and prefill logic
│   ├── gecko_web.py           # Web/HTML rendering for newsletter
│   └── ...                    # Other AWS backend scripts
└── README.md                  # (This file)



--------------------------------------------------------------------------


Architecture


Frontend:
HTML/CSS/JS, fully responsive, modern UX
Externalized CSS and JS for maintainability
Uses favicon as logo, accessible and mobile-optimized
Backend (AWS Lambda/Python):
Newsletter content generation and distribution
DynamoDB (gecko_db) for subscriber storage
GSI (status-index) for efficient queries on subscriber status
Multiple distribution modes: bulk, preview, and HTML view



---------------------------------------------------------------------------


Fair Use Notice


All references to Bud Fox, Wall Street, Charlie Sheen, Gordon Gekko, and Michael Douglas are for commentary and reference only under Fair Use. No copyright ownership is claimed and no money is being made.


--------------------------------------------------------------------------

Author


Scott Gross
scottgross.works