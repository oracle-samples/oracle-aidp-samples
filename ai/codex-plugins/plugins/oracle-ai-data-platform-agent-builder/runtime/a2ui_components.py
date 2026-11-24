"""Build standard A2UI 0.8 controls from the application's action schemas."""


def node(identifier, kind, **props):
    return {'id':identifier,'component':{kind:props}}


def literal(value):
    kind = 'Boolean' if type(value) is bool else 'Number' if type(value) in {int,float} else 'String'
    return {'literal'+kind:value}


def action_nodes(identifier, component, action_id, arguments=None, defaults=None):
    """Schema-driven generation; the wire contains ordinary Button/input nodes."""
    arguments=arguments or {};defaults=defaults or {};action=component['actions'][action_id]
    nodes=[];children=[];context=[{'key':k,'value':literal(v)} for k,v in arguments.items()]
    for key,schema in action['inputSchema']['properties'].items():
        if key in arguments:continue
        field=identifier+'-'+key;path='/inputs/'+identifier+'/'+key
        label={'literalString':schema.get('description') or key}
        if 'enum' in schema:
            chosen=str(defaults.get(key,schema['enum'][0]))
            nodes.extend([node(field+'-label','Text',text=label),node(field,'MultipleChoice',
                selections={'path':path,'literalArray':[chosen]},maxAllowedSelections=1,
                options=[{'label':{'literalString':str(v)},'value':str(v)} for v in schema['enum']])])
            children.append(field+'-label');binding={'path':path+'/0'}
        elif schema['type']=='boolean':
            nodes.append(node(field,'CheckBox',label=label,value={'path':path,'literalBoolean':defaults.get(key,False)}))
            binding={'path':path}
        else:
            nodes.append(node(field,'TextField',label=label,text={'path':path,'literalString':str(defaults.get(key,''))},
                textFieldType='number' if schema['type'] in {'integer','number'} else 'shortText'))
            binding={'path':path}
        children.append(field);context.append({'key':key,'value':binding})
    nodes.append(node(identifier+'-label','Text',text={'literalString':action['label']}))
    nodes.append(node(identifier+'-button','Button',child=identifier+'-label',
        action={'name':component['id']+'.'+action_id,'context':context}))
    children.append(identifier+'-button')
    return [node(identifier,'Column',children={'explicitList':children}),*nodes]
