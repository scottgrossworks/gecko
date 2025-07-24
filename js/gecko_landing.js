// --- Date and Time ---
function pad(n) { return n < 10 ? '0' + n : n; }
function getPSTDate() {
  const now = new Date();
  let utc = now.getTime() + (now.getTimezoneOffset() * 60000);
  let year = now.getFullYear();
  let dstStart = new Date(Date.UTC(year, 2, 8, 10, 0, 0));
  dstStart.setDate(8 + (7 - dstStart.getUTCDay()) % 7);
  let dstEnd = new Date(Date.UTC(year, 10, 1, 9, 0, 0));
  dstEnd.setDate(1 + (7 - dstEnd.getUTCDay()) % 7);
  let isDST = (now >= dstStart && now < dstEnd);
  let offset = isDST ? -7 : -8;
  return new Date(utc + 3600000 * offset);
}
function getGreeting() {
  const hour = getPSTDate().getHours();
  if (hour >= 5 && hour < 12) return "GOOD MORNING";
  if (hour >= 12 && hour < 17) return "GOOD AFTERNOON";
  return "GOOD EVENING";
}

function updateDateTime() {
  const pst = getPSTDate();
  const days = ["SUNDAY","MONDAY","TUESDAY","WEDNESDAY","THURSDAY","FRIDAY","SATURDAY"];
  const months = ["JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE","JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"];
  const dateStr = `${months[pst.getMonth()]} ${pst.getDate()}, ${pst.getFullYear()} - ${days[pst.getDay()]}`;
  document.getElementById('date-str').textContent = dateStr;
  
  // Update greeting
  const greetingElement = document.querySelector('.header-center');
  if (greetingElement) {
    greetingElement.textContent = getGreeting() + "!";
  }
  
  // Update time
  const timeElement = document.querySelector('.header-right');
  if (timeElement) {
    let h = pst.getHours(), m = pst.getMinutes();
    let ampm = h >= 12 ? "PM" : "AM";
    let hour12 = h % 12; 
    if (hour12 === 0) hour12 = 12;
    let min = pad(m);
    timeElement.textContent = `${hour12}:${min} ${ampm}`;
  }
}
setInterval(updateDateTime, 1000);
updateDateTime();

// --- Slideshow ---
const images = document.querySelectorAll('.slide-img');
let idx = 0;
setInterval(() => {
  images[idx].classList.remove('active');
  idx = (idx + 1) % images.length;
  images[idx].classList.add('active');
}, 2000);


// --- Preview Button ---
document.getElementById('preview-btn').addEventListener('click', function() {
  const previewUrl = 'https://unha4lumv1.execute-api.us-west-2.amazonaws.com/prod/web';
  window.open(previewUrl, '_blank');
});


// --- Subscribe Form ---
document.getElementById('subscribe-form').addEventListener('submit', async function(e) {
  e.preventDefault();
  const email = document.getElementById('subscribe-email').value.trim();
  const msgDiv = document.getElementById('subscribe-message');
  const button = document.getElementById('subscribe-btn');
  
  msgDiv.textContent = '';
  if (!email) {
    msgDiv.textContent = 'Please enter your email.';
    msgDiv.style.color = '#ff1616';
    return;
  }
  
  // Disable button and show loading state
  if (button) {
    button.disabled = true;
    button.textContent = 'Subscribing...';
  }
  
  try {
    // Create form data with send_first_issue parameter
    const formData = new FormData();
    formData.append('email', email);
    formData.append('send_first_issue', 'true');
    const params = new URLSearchParams(formData).toString();
    
    const resp = await fetch('https://unha4lumv1.execute-api.us-west-2.amazonaws.com/prod/subscribe?' + params);
    
    if (resp.ok) {
      msgDiv.textContent = 'Welcome to Gekko\'s Birthday! Check your email for updates.';
      msgDiv.style.color = '#39FF14';
      document.getElementById('subscribe-form').reset();
      if (button) {
        button.style.background = 'chartreuse';
        button.style.color = 'black';
        button.textContent = 'Subscribed!';
      }
    } else {
      const data = await resp.json().catch(() => ({}));
      msgDiv.textContent = data.error || 'Subscription failed. Please try again or contact support.';
      msgDiv.style.color = '#ff1616';
      if (button) {
        button.disabled = false;
        button.textContent = 'Subscribe';
      }
    }
  } catch (err) {
    msgDiv.textContent = 'Subscription failed. Please try again or contact support.';
    msgDiv.style.color = '#ff1616';
    if (button) {
      button.disabled = false;
      button.textContent = 'Subscribe';
    }
    console.error('Subscribe error:', err);
  }
});
