function extractVenuePage() {
  const name = document.querySelector('h1')?.textContent?.trim() || '';

  let vendorId = '';
  const vendorImg = document.querySelector('img[src*="/vendors/"]');
  if (vendorImg) {
    const match = vendorImg.src.match(/vendors\/([a-f0-9-]+)/);
    if (match) vendorId = match[1];
  }

  const pdfLinks = [];
  const allLinks = document.querySelectorAll('a[href*=".pdf"]');
  allLinks.forEach(link => {
    const href = link.href;
    const text = link.textContent.trim();
    if (href && text) {
      pdfLinks.push({
        filename: text,
        url: href
      });
    }
  });

  const rooms = [];
  const roomCards = document.querySelectorAll('#packages .bg-white');

  roomCards.forEach(card => {
    const roomName = card.querySelector('h3')?.textContent?.trim() || '';

    const typeBadges = card.querySelectorAll('span[class*="bg-gray-50"]');
    const types = Array.from(typeBadges).map(b => b.textContent.trim()).filter(Boolean);

    let roomId = '';
    const roomImg = card.querySelector('img[src*="/rooms/"]');
    if (roomImg) {
      const match = roomImg.src.match(/rooms\/([a-f0-9-]+)/);
      if (match) roomId = match[1];
    }

    const packages = [];

    const dayLabels = card.querySelectorAll('p.text-\\[10px\\]');

    dayLabels.forEach(dayLabelEl => {
      const dayLabel = dayLabelEl.textContent?.trim() || '';

      const packageContainer = dayLabelEl.nextElementSibling;
      if (!packageContainer) return;

      const packageDivs = packageContainer.querySelectorAll('.bg-gray-50.rounded-lg');

      packageDivs.forEach(pkgDiv => {
        const menuDesc = pkgDiv.querySelector('.text-xs.text-gray-600.line-clamp-2')?.textContent?.trim() || '';

        const capacitySpan = pkgDiv.querySelectorAll('.text-xs.text-gray-600');
        let capacity = '';
        capacitySpan.forEach(span => {
          const text = span.textContent.trim();
          if (/^\d+(-\d+)?$/.test(text)) {
            capacity = text;
          }
        });

        const priceDiv = pkgDiv.querySelector('.text-sm.font-semibold');
        const price = priceDiv?.textContent?.trim() || '';

        let capacityMin = null;
        let capacityMax = null;
        if (capacity) {
          const parts = capacity.split('-');
          capacityMin = parseInt(parts[0]) || null;
          capacityMax = parseInt(parts[1]) || capacityMin;
        }

        if (price) {
          packages.push({
            day: dayLabel,
            menu: menuDesc,
            capacity_min: capacityMin,
            capacity_max: capacityMax,
            price: price
          });
        }
      });
    });

    if (roomName || packages.length > 0) {
      rooms.push({
        name: roomName,
        types: types,
        room_id: roomId,
        packages: packages
      });
    }
  });

  return {
    name,
    vendorId,
    rooms,
    pdfLinks
  };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { extractVenuePage };
}
