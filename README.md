## theLeedz_on_X
## See the results: https://x.com/LeedzLyfe
## Google Gemini powered marketing agent that posts daily to @leedzlyfe on X

############################ architecture
  
This bot is composed of a controller and three modules:

theLeedz_on_X.py -- controller

  trends_handler.py -- calls 3rd party HTTP api to get list of trending topics
                      - packaged as a .zip lambda layer - no dependencies
                      - Environment variables:
                          TRENDS_API = 3rd party API HTTP URL
                                       - https://rapidapi.com/brkygt88/api/twitter-trends5
                                       - free tier --> $2.99 for basically unlimited queries
                          WOEID = used by the API
                          X_KEY = used by the API to get the X trending topics
                          NUM_TRENDS = how many trends to pull --> number of X posts to generate

                      
  gemini_handler.py -- submits structured prompt to Gemini converting topics into statements about the Leedz
                      - built with Docker into a separate lambda, called using invoke(Request_Response)
                      - Environment variables:
                          GOOGLE_API_KEY = Gemini API key
                                          - register for free at https://aistudio.google.com/

                      
  async_X_handler -- async lambda posts a message to X with a postfix
                      -- built with Docker into a separate lambda, called using invoke(Request_Response)
                      - Environment variables:
                          POSTFIX = signature String appended to each sentence before posting
                          API_KEY = required by X -- must have > minimum level access, Pro or above
                          API_SECRET = provied by X Oauth2
                          ACCESS_TOKEN = provided by X Oauth2
                          ACCESS_TOKEN_SECRET = provided by X Oauth2
                          WAIT_SEC = handler will wait random number of seconds between 1-->WAIT_SEC before posting


############################ trends_handler

The current trends API returns 50+ trends in a large JSON data structure that also provides context cues and other metadata.  This handler strips out the essential info and distills everything into a numbered list of trending topics.
The length of the list is NUM_TRENDS.

5/21/2024 -- current algorithm does NOT pick top ( NUMTRENDS ) trends returned from API.  It chooses at random ( NUMTRENDS ) number of topics from the entire list of 50+ returned from the API.  
The API only refreshes every 30 minutes, and the top 5 or so trends may be the same hour-to-hour.  This implementation allows the function to be called as 
a standalone in addition to a cron-job and pick fresh topics each call.  This would be a good area to customize for your own needs.

DOES NOT require additional packages
uses http.client.HTTPSConnection to connect to API

  1. put source code in ./python
  2. create trends_handler.zip in ./ 
  3. upload .zip to your controller as a Lambda layer



############################ gemini_handler

1. Sign up for a Google Gemini account
2. https://aistudio.google.com/app/prompts/new_chat
3. Create a STRUCTURED PROMPT that
    - instructs the AI on the goal -- to turn trends into X posts about the Leedz
    - provides a list of statements about the Leedz to incorporate whole or in part
    - provides a list of SAMPLE request/responses that *I WROTE* to model the output and train the AI
  
Here is the prompt I used:

    prompt_parts = [
      "You are an AI bot who creates posts on X to advertise a business called The Leedz.\nYour input is a list of keywords.\nYou will generate a list of sentences advertising the business containing the keywords.\nEach sentence must be less than 201 characters long.\n\nEach sentence must contain one keyword. \nOnce a keyword has been used, remove it from the list and do not re-use it in another sentence.\nThe first time a keyword is used, preface it with a '#'.\nIf a keyword contains a term in parentheses, use the term for context to clarify the meaning of the keyword. \nFor example,  'Smith (NBA)' indicates that Smith is the name of an NBA player.\nDO NOT include parentheses in the sentence you generate.\n\nTo craft each sentence, use one or more or parts of phrases from About:\nIf a hashtag appears in the About, continue to use it in the output sentence.  \n\nGoals:\n1. Each output sentence should assume that the keyword is true, trending, and important to the reader right now.\n2. Each sentence should promote the Leedz as a business solution to the situation the keyword describes or problem created by the keyword trending now.\n\n\nAbout:\n\n1. theleedz.com is a web app users can access from any browser\n2. The Leedz is a marketplace for #smallbiz vendors to make more money from their booking calendars.\n3. The Leedz solves a problem that every vendor has.  You book a date on your calendar, and afterwards someone else requests your service for the same date. \nThe request has value to another vendor who can take the booking.  Now you can sell it, on the Leedz.\n4. The Leedz enables #smallbusiness owners to make back their advertising money, and grow a network of trusted vendors with whom to trade booking info\n5. The business model of the Leedz is the inverse of #GigSalad, #Thumbtack, #Yelp and other booking providers.  They sell you five gigs for the same date, knowing you can only work one.  Make that money back by selling the other four, on the Leedz.  \n6. #Partyvendors like #inflatables and #facepainters make excellent money on weekends with very low overhead.  These are popular second-jobs, or #sidehustles.\n7. Every day more people are adopting #sidehustles and second jobs to make ends meet in what is called the exploding #gigeconomy\n8. There are over 50 trades on the Leedz from #photobooth vendors, to #caterers, to hair #braiding and #personaltrainers.\n9. The Leedz is built on #serverless #AWS technology and uses #Square to process all payments.  Most #small-business vendors ALREADY use Square to charge sales .  When you sell a leed the money goes directly into your Square account.\n10. The Leedz incorporates industry-standard encryption technology and security protocols to keep your personal and payment data private.",
      "Keywords: 1. Fooball\n2. Ubisoft (software)",
      "Sentences: Spend more time watching #football this season and less time managing your calendar with the Leedz, a web app that helps vendors find new clients and make more money from their existing bookings.\n\nAs #Ubisoft and other software companies announce layoffs and the 'gig economy' grows faster than ever,  the Leedz can help anyone with a side-hustle find work and build a market for their services.",
      "Keywords: 1. Tacos",
      "Sentences: Book the perfect food vendor for your next party, from #tacos to churros, you can find them on the Leedz.",
      "Keywords: 1. dogs\n2. WWE\n3. NBA Playoffs\n4. Dembele (Champions League)\n5. Ethe\n6. Apple (stock market)\n7. Boeing Whistleblower Dead (corporate politics)\n8. earthquake (Tokyo)\n9. layoffs (business)\n10. Mother's Day",
      "Sentences: walking #dogs is a popular #sidehustle.  Use the Leedz to find your next client and network with other vendors with whom you can trade booking info in the future.\n\nAre you hosting a #WWE watch party?  The Leedz can help you find the perfect mix of food and entertainment vendors to make it a true Royal Rumble!\n\nThe #NBA Playoffs is a perfect time for a house party with #catering #balloonart and #caricatures.  Find all those vendors and more on the Leedz.\n\nWhile you're watching the #Dembele score a goal in the Champions League, the Leedz is working to sell the bookings you can't work to vendors who can.\n\nTrading #Ethe is a great way to finance your next small-business #sidehustle.  When you are ready to find your first bookings, join the Leedz.\n\nThe stock market is volatile, but #Apple is a safe bet, and so is The Leedz. \n\nWith news of the #Boeing Whistleblower Death and other corporate politics, people are turning to more secure, reliable home businesses.  Build yours on the Leedz. \n\nDon't let an #earthquake  in Tokyo stop you from running your business.  Grow it on the Leedz.\n\n#Layoffs happen in every industry. The Leedz can help you find new gigs and earn extra income from your #sidehustle.\n\nFind a #florist and #balloonart for #MothersDay on the Leedz, a marketplace for small-business vendors.",
      "Keywords: 1. Sukuna\n2. Yuji\n3. Hardaway (NBA)\n4. Yasuke (Gaming)\n5. Chargers\n6. Darby (Wrestling)\n7. Hannity\n8. Nitro (Technology)\n9. Kenjaku",
      "Sentences: The Leedz can help you maximize your time and resources so you can focus on what matters most: your passion for #Sukuna and the world of Jujutsu Kaisen.  \n\nWith all the excitement surrounding #Yuji, the Leedz helps you book more gigs so you can spend more time enjoying your favorite anime.  \n\nOn the Leedz you can sell your unused dates and make more money from your #sidehustle than #Hardaway or any other player in the NBA.\n\nThe Leedz is the perfect platform for anyone with a #sidehustle in the #gaming industry, like those who love Yasuke, to manage their calendars more efficiently.\n\nThe Leedz is a great way to connect with other party vendors in the #Chargers tailgate community and grow the network for your unique product or service.  \n\nThe Leedz helps you manage your time more efficiently so you can spend more time watching #Darby in the wrestling ring and less time searching for new clients.  \n\nThe Leedz is a great way to make money from your existing bookings, even if you're busy watching #Hannity like millions of others. \n\n#Nitro is the latest technology craze, but the Leedz is the platform that will help you grow your small business. \n\nDon't let #Kenjaku get in the way of your business goals.  The Leedz is a marketplace for party vendors and entertainers to make more money from their booking calendars.",
      "Keywords: " + << STRING - NUMBERED LIST OF TRENDING KEYWORDS >>,
      "Sentences: ",
    ]

    
  1. Build using Docker / AWS CLI
  2. Upload as a separate lambda function and call using lambda.invoke( 'RequestResponse' )



############################ async_x_handler

  1. Build using Docker / AWS CLI
  2. Upload as a separate lambda function and called using lambda.invoke( 'Event' )
  3. See my paper on Asyncrhonous Messaging Design Pattern with Controllers : http://scottgross.works/papers/async

  DO NOT USE A CAPITAL LETTER -- X or otherwise -- in the filename
  
  The Docker build process is difficult.  These are the commands I use on Windows, in Powershell, to build an deploy this and other Lambdas to AWS in region us-west-2:
  1. Start Docker Dekstop
  
  $ aws ecr get-login-password --region <YOUR AWS REGION> | docker login --username AWS --password-stdin <YOUR PID>.dkr.ecr.us-west-2.amazonaws.com
  $ aws ecr create-repository --repository-name gemini_handler --region us-west-2 --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
  $ docker build  --platform linux/x86_64 -t <YOUR PID>.dkr.ecr.us-west-2.amazonaws.com/gemini_handler:latest --no-cache .
  $ docker push <YOUR PID>.dkr.ecr.us-west-2.amazonaws.com/gemini_handler:latest
  $ aws lambda update-function-code --function-name  arn:aws:lambda:us-west-2:<YOUR PID>:function:gemini_handler --image-uri <YOUR PID>.dkr.ecr.us-west-2.amazonaws.com/gemini_handler:latest




############################ Contact

See the results: https://x.com/LeedzLyfe
PLEASE branch this code and make it work for you.  
write me @leedzlyfe and show me what you build.
Ask me anything:  theleedz.com@gmail.com
theleedz.com


  

