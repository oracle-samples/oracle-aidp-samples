export function requestBodyReference(section, operation, fullName) {
  const blocks = [...section.matchAll(/\*\*Request Body \(`([^`]+)`\):\*\*([\s\S]*?)(?=\*\*Request Body|\*\*Example:|\n---|$)/g)];
  // The README sorts models alphabetically, not by their position in the payload.
  const bodyModel = operation ? operation.bodyModel || '' : (blocks.length === 1 ? blocks[0][1] : '');
  if (operation && !bodyModel && blocks.length) {
    throw new Error(`${fullName}: README request models have no root in the CLI manifest.`);
  }
  if (blocks.length > 1 && !operation?.bodyModel) {
    throw new Error(`${fullName}: ambiguous request models; supply the aidp-cli operation_manifest.json as the second argument.`);
  }
  if (!bodyModel) return { bodyModel: '', bodyFields: [] };
  // Empty request models have no Request Body section in the generated README.
  if (!blocks.length && operation?.bodyFields?.length === 0) return { bodyModel, bodyFields: [] };
  const block = blocks.find((entry) => entry[1] === bodyModel);
  if (!block) throw new Error(`${fullName}: root model ${bodyModel} is missing from the README; use matching CLI documentation and manifest versions.`);
  const bodyFields = [...block[2].matchAll(/^- `([^`]+)` \(([^)]+)\) —[ \t]*(.*)$/gm)].map((field) => ({
    name: field[1], type: field[2], description: field[3].trim()
  }));
  if (operation?.bodyFields) {
    // MLflow JSON uses snake_case where the SDK manifest uses camelCase.
    // Compare names without changing the documented wire-format field names.
    const names = (fields) => fields.map((field) => field.name.replaceAll('_', '').toLowerCase()).sort();
    const documented = names(bodyFields);
    const expected = names(operation.bodyFields);
    if (JSON.stringify(documented) !== JSON.stringify(expected)) {
      throw new Error(`${fullName}: root fields differ between the README and CLI manifest.`);
    }
  }
  return { bodyModel, bodyFields };
}
