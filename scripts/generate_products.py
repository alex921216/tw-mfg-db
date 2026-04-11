"""
generate_products.py

Generates products_en and improves company_profile_en for all visible companies
(those with phone or website). Uses rule-based mapping from industry_en and name_zh
keywords — no LLM calls required.
"""

import sqlite3
import re

DB_PATH = 'data/tmdb.db'

INDUSTRY_PRODUCTS = {
    'Electronic Components Manufacturing': 'Semiconductors, ICs, passive components, PCBs, connectors',
    'Other Electronic Components Manufacturing': 'LED chips, display panels, sensors, optical components',
    'Computer, Electronic & Optical Products Manufacturing': 'Computers, servers, networking equipment, optical instruments',
    'Electrical Equipment Manufacturing': 'Motors, transformers, power supplies, cables, switches',
    'Metal Products Manufacturing': 'Metal parts, fasteners, screws, bolts, precision stampings, die castings',
    'Other Metal Products Manufacturing': 'Metal fabrication, surface treatment, plating, heat treatment',
    'General-Purpose Machinery Manufacturing': 'Pumps, compressors, valves, bearings, gears, hydraulic equipment',
    'Machinery & Equipment Manufacturing': 'CNC machines, machine tools, automation equipment, industrial robots',
    'Plastics Products Manufacturing': 'Injection molded parts, plastic packaging, containers, films',
    'Other Plastics Products Manufacturing': 'Custom plastic components, blow molding, extrusion products',
    'Rubber Products Manufacturing': 'Rubber seals, gaskets, O-rings, tires, industrial rubber parts',
    'Food Manufacturing': 'Processed foods, frozen foods, seasonings, beverages, bakery products',
    'Beverage Manufacturing': 'Soft drinks, juices, tea, coffee, alcoholic beverages',
    'Textile Manufacturing': 'Fabrics, yarns, fibers, functional textiles, knitted goods',
    'Apparel & Clothing Manufacturing': 'Garments, sportswear, uniforms, fashion accessories',
    'Chemical Materials & Fertilizers Manufacturing': 'Industrial chemicals, resins, adhesives, solvents, fertilizers',
    'Other Chemical Products Manufacturing': 'Specialty chemicals, coatings, paints, cleaning agents',
    'Pharmaceuticals Manufacturing': 'Pharmaceutical APIs, drug formulations, medical devices',
    'Basic Metal Manufacturing': 'Steel products, aluminum, copper, alloys, metal ingots',
    'Motor Vehicles & Parts Manufacturing': 'Auto parts, engine components, brake systems, transmission parts',
    'Other Transport Equipment Manufacturing': 'Bicycle parts, motorcycle parts, marine equipment, e-bikes',
    'Furniture Manufacturing': 'Office furniture, home furniture, fixtures, cabinets',
    'Pulp, Paper & Paper Products Manufacturing': 'Paper, cardboard, packaging materials, tissue products',
    'Non-Metallic Mineral Products Manufacturing': 'Glass, ceramics, cement, concrete, stone products',
    'Printing & Reproduction of Recorded Media': 'Commercial printing, packaging printing, labels',
    'Wood & Bamboo Products Manufacturing': 'Wood products, bamboo items, plywood, lumber',
    'Leather & Fur Products Manufacturing': 'Leather goods, shoes, bags, belts',
    'Other Manufacturing': 'Miscellaneous manufactured products',
    'Tobacco Manufacturing': 'Tobacco products, cigarettes',
    'Petroleum & Coal Products Manufacturing': 'Petroleum products, petrochemicals, lubricants',
    'Industrial Machinery Repair & Installation': 'Industrial equipment maintenance, installation services',
}

# Maps Chinese name keywords to specific product descriptions
NAME_PRODUCT_MAP = {
    '螺絲': 'screws, bolts, nuts, fasteners',
    '彈簧': 'springs, elastic components',
    '模具': 'molds, dies, tooling',
    '半導體': 'semiconductor devices, wafers, chips',
    'PCB': 'printed circuit boards, HDI boards',
    '面板': 'display panels, LCD/LED modules',
    '觸控': 'touch panels, touchscreen modules',
    '光電': 'optoelectronic components, LEDs, laser modules',
    '光學': 'optical lenses, camera modules, precision optics',
    '電池': 'batteries, battery packs, energy storage',
    '太陽能': 'solar cells, solar panels, PV modules',
    '馬達': 'electric motors, drives, motor controllers',
    '變壓器': 'transformers, power transformers',
    '連接器': 'connectors, cable assemblies',
    '電線電纜': 'power cables, wiring, cable assemblies',
    '自行車': 'bicycle frames, components, accessories',
    '機車': 'motorcycle parts, scooter components',
    '汽車': 'automotive parts, vehicle components',
    '食品': 'food products, processed foods',
    '紡織': 'textiles, fabrics, yarns',
    '鋼鐵': 'steel products, structural steel, rebar',
    '水泥': 'cement, concrete, building materials',
    '玻璃': 'glass products, flat glass, container glass',
    '化工': 'chemicals, industrial chemicals',
    '塑膠': 'plastic parts, plastic products',
    '橡膠': 'rubber products, rubber parts',
    '印刷': 'printing services, packaging printing',
    '包裝': 'packaging materials, containers',
    '精密': 'precision components, precision machining',
    '自動化': 'automation systems, automated equipment',
}

# City-specific context blurbs for profile enrichment
CITY_CONTEXT = {
    'Changhua County': 'part of Taiwan\'s fastener manufacturing hub producing ~50% of global screw exports',
    'Taichung City': 'Taiwan\'s precision machinery and machine tool capital',
    'New Taipei City': 'northern Taiwan\'s largest manufacturing base with diverse industrial output',
    'Taoyuan City': 'Taiwan\'s industrial corridor with excellent logistics access near international airport',
    'Kaohsiung City': 'Taiwan\'s major port city and heavy industry center',
    'Tainan City': 'southern Taiwan\'s electronics and semiconductor cluster',
    'Hsinchu City': 'home to Taiwan\'s premier science park and semiconductor ecosystem',
    'Hsinchu County': 'surrounding Taiwan\'s science park with electronics supply chain',
    'Yunlin County': 'agricultural processing and petrochemical manufacturing hub',
    'Miaoli County': 'precision machinery and materials manufacturing zone',
    'Pingtung County': 'agricultural processing and aquaculture product manufacturing',
    'Nantou County': 'mountainous region known for food processing and wood products',
    'Yilan County': 'food processing and natural resource manufacturing area',
    'Keelung City': 'port city with logistics and light manufacturing',
    'Chiayi City': 'agricultural processing and traditional manufacturing center',
    'Chiayi County': 'agricultural and food product manufacturing base',
    'Taipei City': 'Taiwan\'s capital and high-tech services/R&D center',
    'New Taipei': 'northern Taiwan\'s largest manufacturing base with diverse industrial output',
}


def extract_name_products(name_zh: str) -> list[str]:
    """Extract specific product keywords from Chinese company name."""
    found = []
    for keyword, products in NAME_PRODUCT_MAP.items():
        if keyword in name_zh:
            found.append(products)
    return found


def build_products_en(name_zh: str, industry_en: str) -> str:
    """Build products_en string from industry and name keywords."""
    name_products = extract_name_products(name_zh or '')
    industry_products = INDUSTRY_PRODUCTS.get(industry_en or '', '')

    if name_products:
        # Merge name-derived products first, then add industry base
        all_products = name_products[:]
        if industry_products:
            all_products.append(industry_products)
        return '; '.join(all_products)
    elif industry_products:
        return industry_products
    else:
        return 'Manufactured products'


def build_profile_en(
    name_zh: str,
    industry_en: str,
    city_en: str,
    capital_amount,
    factory_count: int,
    products_en: str,
) -> str:
    """Build an improved company_profile_en with product context."""
    # Lead with product description
    name_products = extract_name_products(name_zh or '')
    if name_products:
        product_lead = name_products[0]
    elif products_en and products_en != 'Manufactured products':
        # Take first clause of products_en (before semicolon or comma limit)
        product_lead = products_en.split(';')[0].strip()
        # Truncate if too long
        if len(product_lead) > 60:
            parts = product_lead.split(',')
            product_lead = ', '.join(parts[:3]).strip()
    else:
        product_lead = None

    if product_lead:
        intro = f'Taiwan-based manufacturer of {product_lead}.'
    else:
        intro = 'Taiwan-based manufacturer.'

    # Industry context
    if industry_en:
        industry_part = f'Specializing in {industry_en}'
    else:
        industry_part = None

    # City context
    city_blurb = CITY_CONTEXT.get(city_en or '', '')
    if city_en and city_blurb:
        location_part = f'with facilities in {city_en}, {city_blurb}'
    elif city_en:
        location_part = f'with facilities in {city_en}'
    else:
        location_part = None

    # Combine industry + location
    if industry_part and location_part:
        detail = f'{industry_part} {location_part}.'
    elif industry_part:
        detail = f'{industry_part}.'
    elif location_part:
        detail = f'Located {location_part}.'
    else:
        detail = None

    # Capital note
    capital_part = None
    if capital_amount and capital_amount > 0:
        usd = capital_amount / 30  # rough TWD→USD
        if capital_amount >= 1_000_000_000:
            twd_str = f'NT${capital_amount / 1_000_000_000:.1f}B'
        elif capital_amount >= 1_000_000:
            twd_str = f'NT${capital_amount / 1_000_000:.0f}M'
        else:
            twd_str = f'NT${capital_amount:,}'
        capital_part = f'Registered capital of {twd_str} (approximately USD {usd / 1_000_000:.1f}M).'

    # Factory count
    facility_part = None
    if factory_count and factory_count > 1:
        facility_part = f'Operates {factory_count} factory facilities across Taiwan.'

    # Assemble
    parts = [intro]
    if detail:
        parts.append(detail)
    if capital_part:
        parts.append(capital_part)
    if facility_part:
        parts.append(facility_part)

    return ' '.join(parts)


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Fetch visible companies
    cursor.execute("""
        SELECT id, name_zh, industry_en, city_en, capital_amount, tax_id
        FROM factories
        WHERE (phone IS NOT NULL AND phone != '' OR website IS NOT NULL AND website != '')
    """)
    companies = cursor.fetchall()
    print(f'Processing {len(companies)} visible companies...')

    # Pre-compute factory counts per tax_id (group companies)
    cursor.execute("""
        SELECT tax_id, COUNT(*) as cnt
        FROM factories
        GROUP BY tax_id
    """)
    factory_counts = {row['tax_id']: row['cnt'] for row in cursor.fetchall()}

    updated = 0
    for row in companies:
        company_id = row['id']
        name_zh = row['name_zh'] or ''
        industry_en = row['industry_en'] or ''
        city_en = row['city_en'] or ''
        capital_amount = row['capital_amount']
        tax_id = row['tax_id']

        factory_count = factory_counts.get(tax_id, 1)

        products_en = build_products_en(name_zh, industry_en)
        profile_en = build_profile_en(
            name_zh, industry_en, city_en, capital_amount, factory_count, products_en
        )

        cursor.execute(
            """
            UPDATE factories
            SET products_en = ?, company_profile_en = ?
            WHERE id = ?
            """,
            (products_en, profile_en, company_id),
        )
        updated += 1

    conn.commit()
    conn.close()

    print(f'Done. Updated {updated} companies.')
    print('  - products_en: filled for all visible companies')
    print('  - company_profile_en: improved with product context')


if __name__ == '__main__':
    main()
