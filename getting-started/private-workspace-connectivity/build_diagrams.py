# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Render original conceptual network diagrams. Build dependency: Pillow."""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).parent
DIAGRAMS = {
    'V01': ('Amazon S3 through customer egress', ['AIDP compute', 'Workspace private\nconnection', 'Customer subnet\nroutes and rules', 'NAT gateway\noutbound HTTPS', 'S3 regional\npublic endpoint'], 'AWS service', ['Illustrative NAT example: the S3 leg uses public internet connectivity.', 'Configure the exact hostname used by the client; no AWS VPC is required for this example.']),
    'V02': ('Customer source restriction through a service gateway', ['AIDP compute', 'Workspace private\nconnection', 'Customer subnet\nservice-CIDR route', 'Service gateway\nsame OCI region', 'OCI service\nsource policy'], 'Regional OCI service', ['The service gateway serves supported services in the same region as the customer VCN.', 'Keep the source restriction unchanged and correlate the operation with independent source/path evidence.']),
    'V03': ('Cross-region Object Storage through customer egress', ['AIDP compute\nregion A', 'Workspace private\nconnection', 'Customer subnet\nregion A', 'NAT gateway\npublic egress', 'Object Storage\nregion B'], 'Remote OCI region', ['This public-endpoint example uses customer egress; the region A service gateway is not a region B route.', 'Verify the client uses region B and validate an independent region A control read.']),
    'V04': ('Service private endpoint connectivity', ['AIDP compute', 'Workspace private\nconnection', 'Customer subnet\nprivate routes', 'Private endpoint\ntarget security', 'Database or\nprivate service'], 'Private destination', ['Use the private descriptor from the service and preserve the client hostname for certificate checking.', 'If another VCN is involved, add the appropriate peering/DRG routes and return path.']),
    'V05': ('Check each selected service hostname and route', ['AIDP compute', 'Workspace private\nconnection', 'Customer subnet\nDNS and security', 'Service-specific\nnetwork route', 'Selected OCI\nservice endpoint'], 'Selected service', ['Repeat with actual service resource hostnames and their own ports/routes.', 'This is a coverage checklist, not a claim that all service zones are automatically configured.']),
    'V06': ('DNS edits with disposable, owned test endpoints', ['AIDP compute', 'Workspace private\nconnection', 'Customer subnet\nexisting routes', 'Approved route\nto test targets', 'Owned endpoint\nA or B'], 'Owned test targets', ['Add A, replace with B, delete the test entry, and reopen the original configuration.', 'Changing a hostname does not create a gateway, open a firewall, or grant service permissions.']),
    'V07': ('Observe DNS changes in the same Spark application', ['Same Spark\napplication', 'Workspace private\nconnection', 'Customer subnet\nunchanged routes', 'Existing route\nand security', 'Owned fixture\nendpoint A or B'], 'Test destination', ['Only attempt a live edit when supported by the deployment; otherwise record the prerequisite as blocked.', 'Record application identity and lifecycle evidence before and after each permitted DNS change.']),
}

def font(size, bold=False):
    for name in (('DejaVuSans-Bold.ttf', 'Arial Bold.ttf') if bold else ('DejaVuSans.ttf', 'Arial.ttf')):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            pass
    return ImageFont.load_default()

def build(case, data):
    title, labels, destination, notes = data
    image = Image.new('RGB', (1500, 440), 'white')
    d = ImageDraw.Draw(image)
    d.text((30, 24), case + ' - ' + title, fill='#183d4a', font=font(29, True))
    d.text((30, 74), 'Conceptual topology. Arrows show connection initiation; responses use the established connection.', fill='#4e626b', font=font(19))
    for x, width, label, fill in [(15, 560, 'AIDP managed environment', '#eaf1f5'), (595, 560, 'Customer-controlled network', '#e9f3ee'), (1175, 310, destination, '#f5f1e7')]:
        d.rounded_rectangle((x, 120, x+width, 310), radius=12, fill=fill, outline='#cad5d9', width=2)
        d.text((x+16, 133), label, fill='#35545e', font=font(20, True))
    for i, label in enumerate(labels):
        x = 35 + i*290
        d.rounded_rectangle((x, 180, x+250, 285), radius=8, fill='white', outline='#307160', width=2)
        lines = label.split('\n')
        for j, line in enumerate(lines):
            bbox = d.textbbox((0, 0), line, font=font(22, True))
            d.text((x+125-(bbox[2]-bbox[0])/2, 210+j*28), line, fill='#203d43', font=font(22, True))
        if i < 4:
            d.line((x+250, 233, x+286, 233), fill='#307160', width=3)
            d.polygon([(x+286, 233), (x+276, 226), (x+276, 240)], fill='#307160')
    for i, note in enumerate(notes):
        d.text((30, 338+i*31), note, fill='#425b65', font=font(18))
    d.text((30, 413), 'DNS configuration, packet routing and service authorization are separate checks.', fill='#425b65', font=font(16))
    (ROOT / 'assets').mkdir(exist_ok=True)
    image.save(ROOT / 'assets' / (case.lower() + '-network.png'))

def resolution_diagram(private_only=False):
    """Separate configuration/DNS relationships from application connections."""
    width, height = 1800, (650 if private_only else 1040)
    image = Image.new('RGB', (width, height), 'white')
    d = ImageDraw.Draw(image)
    d.text((35, 25), 'V04 - Default private DNS proxy path' if private_only else 'DNS resolution paths and application connections', fill='#183d4a', font=font(34, True))
    d.text((35, 80), 'Conceptual paths: dashed blue = DNS/provisioning; solid green = application connection.', fill='#4e626b', font=font(22))

    def row(y, labels, color, dashed=False):
        for i, label in enumerate(labels):
            x = 35 + i * 355
            d.rounded_rectangle((x, y, x+305, y+95), radius=10, fill='#f4f8fa' if dashed else '#edf6f0', outline=color, width=2)
            for j, line in enumerate(label.split('\n')):
                bounds = d.textbbox((0, 0), line, font=font(23, True))
                assert bounds[2]-bounds[0] < 295, line
                d.text((x+152-(bounds[2]-bounds[0])/2, y+19+j*29), line, fill='#203d43', font=font(23, True))
            if i < 4:
                a, b, mid = x+305, x+350, y+47
                if dashed:
                    for point in range(a,b,10): d.line((point,mid,min(point+5,b),mid), fill=color, width=3)
                else: d.line((a,mid,b,mid), fill=color,width=3)
                d.polygon([(b,mid),(b-10,mid-7),(b-10,mid+7)],fill=color)

    offset = 0
    if not private_only:
        d.text((35, 142), 'A. Explicit Advanced DNS: service-resolvable public hostname requiring customer-VCN routing', fill='#246585', font=font(26, True))
        row(195, ['Public client\nhostname', 'Service-side\nprovisioning resolution', 'Explicit hostname\nmapping', 'Runtime mapped\nDNS answer', 'Client uses\noriginal hostname'], '#246585', True)
        row(330, ['AIDP application', 'Workspace private\nconnection', 'Customer subnet', 'Approved customer\nroute', 'Public service\nendpoint'], '#307160')
        d.text((35, 450), 'Provisioning and runtime DNS are distinct steps; inspect mapping/readback and actual workload results.', fill='#425b65', font=font(22))
        offset = 405
    d.text((35, 145+offset), 'B. Private ADB hostname unresolved by service resolver: default private DNS proxy path', fill='#246585', font=font(26, True))
    row(205+offset, ['Private ADB\nhostname', 'Workspace DNS\nresolution', 'Private-endpoint\nDNS proxy', 'Customer DNS\nresolver', 'Private target\nanswer / mapping'], '#246585', True)
    row(345+offset, ['AIDP application', 'Workspace private\nconnection', 'Customer subnet', 'Private routes\nand target security', 'ADB private\nendpoint'], '#307160')
    d.text((35, 475+offset), 'Add no test-specific Advanced DNS entry solely because the hostname is private. Check existing overrides.', fill='#425b65', font=font(22))
    d.text((35, 520+offset), 'Keep the hostname for TLS identity checks. A mapped DNS answer need not equal the console private IP.', fill='#425b65', font=font(22))
    d.text((35, height-48), 'Regional service-zone allowances are a separate policy mechanism; none of these paths grants service permissions.', fill='#425b65', font=font(21))
    image.save(ROOT / 'assets' / ('v04-network.png' if private_only else 'dns-resolution-paths.png'))


def bucket_and_adb_diagram():
    """Two application destinations with their distinct source controls."""
    image = Image.new('RGB', (1500, 600), 'white')
    d = ImageDraw.Draw(image)
    d.text((30, 24), 'V02 - One customer VCN, two service access checks', fill='#183d4a', font=font(30, True))
    d.text((30, 76), 'Connection paths through the same-region service gateway', fill='#4e626b', font=font(22))
    def box(rect, lines, color='#e9f3ee'):
        d.rounded_rectangle(rect, radius=12, fill=color, outline='#307160', width=2)
        for j, line in enumerate(lines):
            bounds = d.textbbox((0, 0), line, font=font(22, True))
            assert bounds[2]-bounds[0] < rect[2]-rect[0]-20, line
            d.text(((rect[0]+rect[2]-bounds[2]+bounds[0])/2, rect[1]+23+j*29), line,
                   fill='#203d43', font=font(22, True))
    def arrow(points):
        d.line(points, fill='#307160', width=4)
        x,y=points[-1];d.polygon([(x,y),(x-12,y-8),(x-12,y+8)],fill='#307160')
    box((30, 235, 270, 350), ['AIDP compute', 'CSV + SQL reads'], '#eaf1f5')
    box((320, 220, 640, 365), ['Workspace private', 'connection', 'Customer source subnet'])
    box((690, 220, 960, 365), ['Service gateway', 'All regional services', 'Oracle network'])
    box((1040, 130, 1470, 285), ['Private Object Storage bucket', 'HTTPS 443', 'IAM Network Source + reader'], '#f5f1e7')
    box((1040, 340, 1470, 495), ['ADB public endpoint', 'TCPS descriptor port', 'VCN ACL + database login'], '#f5f1e7')
    arrow([(270,292),(320,292)]);arrow([(640,292),(690,292)])
    arrow([(960,292),(1000,292),(1000,207),(1040,207)])
    arrow([(960,292),(1000,292),(1000,417),(1040,417)])
    d.text((30, 410), 'Use the exact public client hostnames in the workspace configuration.', fill='#425b65', font=font(20))
    d.text((30, 450), 'Keep IAM, database ACLs, identities and routes fixed between phases.', fill='#425b65', font=font(20))
    d.text((30, 540), 'The IAM Network Source controls bucket authorization; the ADB VCN ACL controls SQL connection origins.', fill='#425b65', font=font(20))
    image.save(ROOT / 'assets' / 'v02-network.png')


if __name__ == '__main__':
    for key, value in DIAGRAMS.items():
        if key not in ('V02', 'V04'):
            build(key, value)
    bucket_and_adb_diagram()
    resolution_diagram()
    resolution_diagram(private_only=True)
