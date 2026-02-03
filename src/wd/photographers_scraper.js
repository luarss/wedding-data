function scrapePhotographerPage() {
  const name = document.querySelector('h1')?.textContent?.trim() || '';

  let vendorId = null;
  const vendorImg = document.querySelector('img[src*="/vendors/"]');
  if (vendorImg) {
    const match = vendorImg.src.match(/\/vendors\/([a-f0-9-]+)/);
    if (match) vendorId = match[1];
  }

  const businessType = 'photographer';

  const contact = {
    email: null,
    whatsapp_number: null
  };

  const emailLink = document.querySelector('a[href^="mailto:"]');
  if (emailLink) {
    contact.email = emailLink.href.replace('mailto:', '').trim();
  }

  const whatsappLink = document.querySelector('a[href*="wa.me"], a[href*="whatsapp"]');
  if (whatsappLink) {
    const match = whatsappLink.href.match(/(?:wa\.me\/|phone=)(\d+)/);
    if (match) contact.whatsapp_number = match[1];
  }

  const social = {
    website: null,
    instagram: null,
    facebook: null
  };

  const websiteLink = document.querySelector('a[href*="http"]:not([href*="wedded.sg"]):not([href*="wa.me"]):not([href*="instagram"]):not([href*="facebook"])');
  if (websiteLink) {
    social.website = websiteLink.href;
  }

  const instagramLink = document.querySelector('a[href*="instagram.com"]');
  if (instagramLink) {
    social.instagram = instagramLink.href;
  }

  const facebookLink = document.querySelector('a[href*="facebook.com"]');
  if (facebookLink) {
    social.facebook = facebookLink.href;
  }

  let description = '';
  const descriptionEl = document.querySelector('p.text-gray-600, div[class*="description"], .bio');
  if (descriptionEl) {
    description = descriptionEl.textContent?.trim() || '';
  }

  const packages = [];
  const packageCards = document.querySelectorAll('.border.border-gray-200.rounded-lg');

  packageCards.forEach(card => {
    const titleEl = card.querySelector('h3');
    const priceEl = card.querySelector('.text-3xl.font-bold.text-primary');

    if (titleEl && priceEl) {
      const title = titleEl.textContent?.trim() || '';
      const price = priceEl.textContent?.trim() || '';

      const siblings = Array.from(card.children);
      let duration = '';
      let details = '';

      siblings.forEach(sib => {
        const text = sib.textContent?.trim() || '';
        if (text.includes('hour') && !duration) {
          duration = text;
        }
        if (sib.className.includes('space-y-3') && sib.className.includes('text-sm')) {
          details = text;
        }
      });

      packages.push({
        title: title,
        duration: duration,
        price: price,
        details: details
      });
    }
  });

  const portfolio = {
    photobook_count: 0,
    sample_images: []
  };

  const photobookLinks = document.querySelectorAll('a[href*="/photobooks/"]');
  portfolio.photobook_count = photobookLinks.length;

  const portfolioImages = document.querySelectorAll('img[src*="/vendors/"][src*="/image-"]');
  portfolioImages.forEach(img => {
    if (img.src) {
      portfolio.sample_images.push(img.src);
    }
  });

  const reviews = {
    average_rating: null,
    review_count: 0
  };

  const ratingEl = document.querySelector('[class*="rating"], .stars, [class*="review-score"]');
  if (ratingEl) {
    const ratingMatch = ratingEl.textContent.match(/(\d+\.?\d*)/);
    if (ratingMatch) {
      reviews.average_rating = parseFloat(ratingMatch[1]) || null;
    }
  }

  const reviewCountEl = document.querySelector('[class*="review-count"], [class*="reviews"]');
  if (reviewCountEl) {
    const countMatch = reviewCountEl.textContent.match(/(\d+)/);
    if (countMatch) {
      reviews.review_count = parseInt(countMatch[1]) || 0;
    }
  }

  return {
    name,
    vendorId,
    businessType,
    contact,
    social,
    description,
    packages,
    portfolio,
    reviews
  };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { scrapePhotographerPage };
}
