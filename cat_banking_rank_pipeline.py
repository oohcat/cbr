# This sofware computes the activation matrix for OBIE, CDR, PSD2 and BIAN V12 API.
# It is referenced by the ACT2026 submission titled "The Empirical Universal Quotient of Banking APIs:
# Empirical Category Theory and Complexity Collapse".
# Copyright 2026 Christopher Doyle

import os
import re
import sys
import json
import glob
import csv
import urllib.request

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

try:
    import requests
    import yaml
    import numpy as np
except ImportError:
    print("pip install requests pyyaml numpy")
    sys.exit(1)

DIMENSIONS = ['A', 'T', 'P', 'C', 'B', 'D', 'S', 'Y', 'R', 'F', 'I', 'V', 'L', 'M']

DIM_NAMES = {
    'A': 'AccountState', 'T': 'TransactionLog', 'P': 'PaymentInstruction',
    'C': 'ConsentRecord', 'B': 'BeneficiaryRecord', 'D': 'DirectDebitMandate',
    'S': 'StandingOrder', 'Y': 'PartyIdentity', 'R': 'ProductDefinition',
    'F': 'FundsAvailability', 'I': 'ServiceDiscovery', 'V': 'SecuritiesPosition',
    'L': 'CreditFacility', 'M': 'MarketPrice'
}

FROZEN_PATTERNS = [
    (r'.*(account(?!.?reconcil)|balance|account_status|account_id).*', 'A'),
    (r'.*(transactions|history|journal|entries|statement).*', 'T'),
    (r'.*(payment_order|transfer|instruction|pisp|domestic.payment|international.payment|file.payment|vrp).*', 'P'),
    (r'.*(consent|permission|scope|arrangement).*', 'C'),
    (r'.*(beneficiar|payee|trusted_payee).*', 'B'),
    (r'.*(mandate|direct.debit|directdebit).*', 'D'),
    (r'.*(standing.order|scheduled.payment|periodic).*', 'S'),
    (r'.*(parties|/party|partyidentity|kyc|customer.detail|customerdetail).*', 'Y'),
    (r'.*(product|rate.table|offer|pricing).*', 'R'),
    (r'.*(availability|check.funds|funds.confirm).*', 'F'),
    (r'.*(discovery|capabilities|outages|status).*', 'I'),
    (r'.*(securities|custody|investmentportfolio|equityholding|bondholding|isin|sedol|tradingbook|cusip|instrumentposition|tradeclearing).*', 'V'),
    (r'.*(facility|covenant|credit.limit|drawdown|commercial.loan|syndication|loan.account).*', 'L'),
    (r'.*(market.?rate|fx.?spot|price.?feed|exchange.?rate|foreign.?exchange|spot.?rate|indicative.?rate|rate.?observation|quotation).*', 'M'),
]

EXTENDED_PATTERNS = [
    (r'.*(nostro.?account|account.?reconcil|cash.?position|notional.?pool|pooling).*', 'A'),
    (r'.*(ledger|posting|journal.?entry|affirmation|trade.?confirm|reconcil).*', 'T'),
    (r'.*(clearing|payment.?rail|remittance|netting|disbursement|payment.?execution|payment.?initiation).*', 'P'),
    (r'.*(hedge.?fund|mutual.?fund|unit.?trust|fund.?admin|nav\b|net.?asset.?value|fund.?unit|asset.?management|sub.?custodian|stock.?lending|repo\b|corporate.?action|program.?trading|algorithmic.?trading).*', 'V'),
    (r'.*(correspondent.?bank|counterparty|nostro|vostro|legal.?entity|entity.?directory).*', 'Y'),
    (r'.*(mortgage|consumer.?loan|corporate.?lease|leasing|credit.?card.?facility|credit.?facility|project.?finance|securitization|asset.?securit).*', 'L'),
]

SPECS = {
    'OBIE_AISP': {
        'url': 'https://raw.githubusercontent.com/OpenBankingUK/read-write-api-specs/master/dist/openapi/account-info-openapi.yaml',
        'cache': 'cache_obie_aisp.yaml',
        'label': 'OBIE v3.1 AISP',
        'corpus': 'OBIE',
    },
    'OBIE_PISP': {
        'url': 'https://raw.githubusercontent.com/OpenBankingUK/read-write-api-specs/master/dist/openapi/payment-initiation-openapi.yaml',
        'cache': 'cache_obie_pisp.yaml',
        'label': 'OBIE v3.1 PISP',
        'corpus': 'OBIE',
    },
    'OBIE_CBPII': {
        'url': 'https://raw.githubusercontent.com/OpenBankingUK/read-write-api-specs/master/dist/openapi/confirmation-funds-openapi.yaml',
        'cache': 'cache_obie_cbpii.yaml',
        'label': 'OBIE v3.1 CBPII',
        'corpus': 'OBIE',
    },
    'OBIE_VRP': {
        'url': 'https://raw.githubusercontent.com/OpenBankingUK/read-write-api-specs/master/dist/openapi/vrp-openapi.yaml',
        'cache': 'cache_obie_vrp.yaml',
        'label': 'OBIE v3.1 VRP',
        'corpus': 'OBIE',
    },
    'CDR_BANKING': {
        'url': 'https://raw.githubusercontent.com/ConsumerDataStandardsAustralia/standards/master/swagger-gen/api/cds_banking.json',
        'cache': 'cache_cdr_banking.json',
        'label': 'AU CDR v1.28 Banking',
        'corpus': 'CDR',
    },
    'CDR_COMMON': {
        'url': 'https://raw.githubusercontent.com/ConsumerDataStandardsAustralia/standards/master/swagger-gen/api/cds_common.json',
        'cache': 'cache_cdr_common.json',
        'label': 'AU CDR v1.28 Common',
        'corpus': 'CDR',
    },
    'PSD2_BERLIN': {
        'url': 'https://gitlab.com/the-berlin-group/nextgenpsd2/-/raw/main/Core%20PSD2%20Compliancy/psd2-api_v1.3.16-2025-11-27.openapi.yaml',
        'local_file': 'psd2-api_v1.3.16-2025-11-27.openapi.yaml',
        'cache': 'cache_psd2_berlin.yaml',
        'label': 'PSD2 Berlin Group',
        'corpus': 'PSD2',
    }}

BIAN_YAMLS_DIR = 'bian_yamls_v12'

def load_spec(key):
    meta = SPECS[key]
    cache = meta['cache']
    if os.path.exists(cache):
        print(f"  [cache] {meta['label']}")
        with open(cache, encoding='utf-8', errors='replace') as f:
            raw = f.read()
    else:
        print(f"  [fetch] {meta['label']} ...")
        r = requests.get(meta['url'], timeout=30)
        r.raise_for_status()
        raw = r.text
        with open(cache, 'w', encoding='utf-8', errors='replace') as f:
            f.write(raw)
    if cache.endswith('.yaml'):
        return yaml.safe_load(raw)
    else:
        return json.loads(raw)

def resolve_ref(ref_str, spec):
    if not ref_str.startswith('#/'):
        return {}
    parts = ref_str.lstrip('#/').split('/')
    node = spec
    for p in parts:
        if isinstance(node, dict) and p in node:
            node = node[p]
        else:
            return {}
    return node if isinstance(node, dict) else {}

def extract_strings_from_schema(schema, depth=0, max_depth=4):
    if depth > max_depth or not isinstance(schema, dict):
        return []
    strings = []
    if '$ref' in schema and isinstance(schema['$ref'], str):
        ref_name = schema['$ref'].split('/')[-1]
        strings.append(ref_name)
        return strings
    for field in ('title', 'description', 'name', 'summary'):
        if field in schema and isinstance(schema[field], str):
            strings.append(schema[field])
    if 'enum' in schema and isinstance(schema['enum'], list):
        strings.extend(str(v) for v in schema['enum'])
    if 'properties' in schema and isinstance(schema['properties'], dict):
        for prop_name, prop_schema in schema['properties'].items():
            strings.append(prop_name)
            strings.extend(extract_strings_from_schema(prop_schema, depth+1, max_depth))
    for key in ('items', 'allOf', 'anyOf', 'oneOf'):
        val = schema.get(key)
        if isinstance(val, dict):
            strings.extend(extract_strings_from_schema(val, depth+1, max_depth))
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    strings.extend(extract_strings_from_schema(item, depth+1, max_depth))
    return strings

def flatten_endpoint(path, method, operation, spec):
    parts = [path]
    for field in ('operationId', 'summary', 'description'):
        if field in operation and isinstance(operation[field], str):
            parts.append(operation[field])

    for param in operation.get('parameters', []):
        if isinstance(param, dict):
            if '$ref' in param:
                param = resolve_ref(param['$ref'], spec)
            parts.append(param.get('name', ''))
            parts.append(param.get('description', ''))
            if 'schema' in param:
                parts.extend(extract_strings_from_schema(param['schema']))

    rb = operation.get('requestBody', {})
    for content_type, content in rb.get('content', {}).items():
        parts.extend(extract_strings_from_schema(content.get('schema', {})))

    for status, response in operation.get('responses', {}).items():
        if isinstance(response, dict):
            for content_type, content in response.get('content', {}).items():
                parts.extend(extract_strings_from_schema(content.get('schema', {})))

    return ' '.join(str(p) for p in parts if p)

def extract_endpoints(spec, source_label):
    endpoints = []
    paths = spec.get('paths', {})
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method in ('get', 'post', 'put', 'delete', 'patch'):
            operation = path_item.get(method)
            if not operation:
                continue
            signal = flatten_endpoint(path, method, operation, spec)
            label = f"{source_label}|{method.upper()} {path}"
            endpoints.append((label, signal))
    return endpoints

def load_bian_specs():
    os.makedirs(BIAN_YAMLS_DIR, exist_ok=True)
    yamls = sorted(glob.glob(os.path.join(BIAN_YAMLS_DIR, '*.yaml')))
    
    if not yamls:
        print("  [fetch] BIAN Service Landscape v12.0 ...")
        api_url = 'https://api.github.com/repos/bian-official/public/contents/release12.0.0/semantic-apis/oas3/yamls'
        req = urllib.request.Request(api_url)
        req.add_header('User-Agent', 'Cat-Banking-Pipeline')
        
        try:
            with urllib.request.urlopen(req) as r:
                files = json.loads(r.read())
                
            for f in files:
                if f['name'].endswith('.yaml'):
                    file_path = os.path.join(BIAN_YAMLS_DIR, f['name'])
                    with urllib.request.urlopen(f['download_url']) as r_file:
                        with open(file_path, 'wb') as out_f:
                            out_f.write(r_file.read())
            
            yamls = sorted(glob.glob(os.path.join(BIAN_YAMLS_DIR, '*.yaml')))
            print(f"  [fetch] Downloaded {len(yamls)} BIAN yaml files.")
        except Exception as e:
            print(f"  [error] BIAN fetch failed. {e}")
            return []
    else:
        print(f"  [cache] BIAN Service Landscape v12.0 ({len(yamls)} files)")

    loaded = []
    for path in yamls:
        name = os.path.splitext(os.path.basename(path))[0]
        try:
            with open(path, encoding='utf-8', errors='replace') as f:
                spec = yaml.safe_load(f)
            label = f"BIAN r12|{name}"
            loaded.append((label, spec))
        except Exception:
            pass
            
    return loaded

def decompose(signal, corpus):
    s = signal.lower()
    dims = {sym for pattern, sym in FROZEN_PATTERNS if re.search(pattern, s)}
    if corpus == 'BIAN':
        dims |= {sym for pattern, sym in EXTENDED_PATTERNS if re.search(pattern, s)}
    return dims

def build_matrix(endpoints, corpus_map):
    n, d = len(endpoints), len(DIMENSIONS)
    M = np.zeros((n, d), dtype=np.uint8)
    activations = []
    for i, (label, signal) in enumerate(endpoints):
        corpus = corpus_map.get(i, 'UNKNOWN')
        activated = decompose(signal, corpus)
        activations.append(activated)
        for j, dim in enumerate(DIMENSIONS):
            if dim in activated:
                M[i, j] = 1
    return M, activations

def gf2_rank(M):
    A = M.copy().astype(np.int32)
    rows, cols = A.shape
    rank, pivot_row = 0, 0
    for col in range(cols):
        found = next((r for r in range(pivot_row, rows) if A[r, col] == 1), None)
        if found is None:
            continue
        A[[pivot_row, found]] = A[[found, pivot_row]]
        for r in range(rows):
            if r != pivot_row and A[r, col] == 1:
                A[r] = (A[r] + A[pivot_row]) % 2
        pivot_row += 1
        rank += 1
    return rank

def report(all_endpoints, all_activations, M, corpus_map, target_corpora, title):
    idx = [i for i in range(len(all_endpoints)) if corpus_map.get(i) in target_corpora]
    if not idx:
        return 0, {}, []
    
    sub_M = M[idx]
    sub_act = [all_activations[i] for i in idx]
    sub_eps = [all_endpoints[i] for i in idx]

    rank = gf2_rank(sub_M)
    coverage = {d: int(sub_M[:, j].sum()) for j, d in enumerate(DIMENSIONS)}
    never = [d for d in DIMENSIONS if coverage[d] == 0]
    gaps = [sub_eps[i][0] for i in range(len(sub_eps)) if not sub_act[i]]

    print(f"\n{'='*60}")
    print(f"CORPUS {title}")
    print(f"  Endpoints {len(idx)}  |  Rank (GF2) {rank}")
    print(f"{'='*60}")
    print("  Dimension coverage")
    
    max_coverage = max(coverage.values()) if coverage else 1
    
    for d in DIMENSIONS:
        count = coverage[d]
        if count == 0:
            bar_len = 0
        else:
            bar_len = max(1, int(round((count / max_coverage) * 40)))
        bar = '#' * bar_len
        flag = '  *** NOT ACTIVATED ***' if count == 0 else ''
        print(f"    {d} {DIM_NAMES[d]:<22} {count:>4}  {bar}{flag}")
    
    if never:
        never_str = ', '.join(never)
        print(f"\n  NOT ACTIVATED {never_str}")
    
    pct = 100 * len(gaps) / max(len(idx), 1)
    print(f"\n  Gaps (no dimension) {len(gaps)} / {len(idx)} endpoints ({pct:.0f}%)")
    
    return rank, coverage, never

def verify_zero_dark_endpoints(tsv_path):
    dark_endpoints = []
    with open(tsv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            corpus = row['corpus']
            if corpus in ['OBIE', 'CDR', 'PSD2']:
                dims = row['activated_dims'].strip()
                if not dims:
                    dark_endpoints.append(row['endpoint'])

    print("Zero Dark Endpoint Verification")
    print("===============================")
    if not dark_endpoints:
        print("Status Verified.")
        print("The retail and regulatory corpora contain zero dark endpoints.")
    else:
        print("Status Failed.")
        print(f"Discovered {len(dark_endpoints)} dark endpoints.")
        for ep in dark_endpoints[:10]:
            print(f"  {ep}")

def main():
    print("Cat-Banking Rank Derivation Pipeline — Unified Architecture")
    print("=" * 65)
    
    all_endpoints = []
    corpus_map = {}

    # All corpora (OBIE, CDR, PSD2) are now seamlessly processed through SPECS
    for key, meta in SPECS.items():
        try:
            spec = load_spec(key)
            eps = extract_endpoints(spec, meta['label'])
            print(f"    {meta['label']}: {len(eps)} endpoints")
            for ep in eps:
                corpus_map[len(all_endpoints)] = meta['corpus']
                all_endpoints.append(ep)
        except Exception as e:
            print(f"    SKIPPED {meta['label']}: {e}")

    bian_specs = load_bian_specs()
    bian_count = 0
    for label, spec in bian_specs:
        eps = extract_endpoints(spec, label)
        for ep in eps:
            corpus_map[len(all_endpoints)] = 'BIAN'
            all_endpoints.append(ep)
        bian_count += len(eps)
        
    if bian_specs:
        print(f"    BIAN total: {bian_count} endpoints across {len(bian_specs)} service domains")

    if not all_endpoints:
        sys.exit(1)

    M, activations = build_matrix(all_endpoints, corpus_map)

    rank_obie, _, _ = report(all_endpoints, activations, M, corpus_map, ['OBIE'], 'OBIE')
    rank_cdr, _, _ = report(all_endpoints, activations, M, corpus_map, ['CDR'], 'CDR')
    
    rank_retail, _, _ = report(all_endpoints, activations, M, corpus_map, ['OBIE', 'CDR'], 'OBIE u CDR  (Q_retail)')
    
    if any(c == 'PSD2' for c in corpus_map.values()):
        rank_psd2, _, _ = report(all_endpoints, activations, M, corpus_map, ['PSD2'], 'PSD2 Berlin Group')
        rank_combined, _, _ = report(all_endpoints, activations, M, corpus_map, ['OBIE', 'CDR', 'PSD2'], 'OBIE u CDR u PSD2  (Perturbation)')
        
        delta = rank_combined - rank_retail
        print(f"\n{'='*60}")
        print("PERTURBATION RESULT")
        print(f"{'='*60}")
        print(f"  Rank(PSD2 alone):        {rank_psd2:2d} / 14")
        print(f"  Rank(Q_retail):          {rank_retail:2d} / 14  (baseline)")
        print(f"  Rank(Q_retail u PSD2):   {rank_combined:2d} / 14")
        print(f"  Delta:                   {delta:+d}")
        print()

    has_bian = any(c == 'BIAN' for c in corpus_map.values())
    if has_bian:
        rank_bian, _, _ = report(all_endpoints, activations, M, corpus_map, ['BIAN'], 'BIAN (wholesale)')
        rank_full_union, _, _ = report(all_endpoints, activations, M, corpus_map, ['OBIE', 'CDR', 'BIAN'], 'OBIE u CDR u BIAN  (Q_full)')

    out_tsv = 'cat_banking_activation_detail_bian_12_v4_4.tsv'
    with open(out_tsv, 'w', encoding='utf-8') as f:
        f.write('corpus\tendpoint\t' + '\t'.join(DIMENSIONS) + '\tactivated_dims\n')
        for i, (lbl, _) in enumerate(all_endpoints):
            corpus = corpus_map.get(i, 'UNKNOWN')
            row_vals = '\t'.join(str(M[i, j]) for j in range(len(DIMENSIONS)))
            dims = ','.join(d for d in DIMENSIONS if d in activations[i])
            f.write(f"{corpus}\t{lbl}\t{row_vals}\t{dims}\n")

    verify_zero_dark_endpoints(out_tsv)

if __name__ == '__main__':
    main()
