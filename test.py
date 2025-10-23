from doc_store.doc_client import DocClient
from doc_store.doc_store import DocStore
from doc_store.interface import (
    BlockInput,
    ContentBlockInput,
    ContentInput,
    DocExistsError,
    ElementExistsError,
    ElementNotFoundError,
    LayoutInput,
    MetricInput,
    TaskInput,
    ValueInput,
)

store = DocClient(server_url="http://127.0.0.1:8081")
# store = DocStore()


print("# Read Docs")

print(" - Find 1 doc")
test_doc = next(iter(store.find_docs()))

print(" - Find 10 docs")
test_docs = list(store.find_docs(limit=10))
assert len(test_docs) == 10

###

print(" - Get doc by ID")
doc = store.get_doc(test_doc.id)
assert doc.pdf_path == test_doc.pdf_path

print(" - Try get doc by ID")
doc = store.try_get_doc(test_doc.id)
assert doc is not None and doc.pdf_path == test_doc.pdf_path

print(" - Get doc by non-exist ID")
try:
    store.get_doc("non-exist-page-id")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get doc by non-exist ID")
assert store.try_get_doc("non-exist-page-id") is None

###

print(" - Get doc by PDF path")
doc = store.get_doc_by_pdf_path(test_doc.pdf_path)
assert doc.id == test_doc.id

print(" - Try get doc by PDF path")
doc = store.try_get_doc_by_pdf_path(test_doc.pdf_path)
assert doc is not None and doc.id == test_doc.id

print(" - Get doc by non-exist PDF path")
try:
    store.get_doc_by_pdf_path("non/exist/path.pdf")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get doc by non-exist PDF path")
assert store.try_get_doc_by_pdf_path("non/exist/path.pdf") is None

###

print(" - Get doc by PDF hash")
doc = store.get_doc_by_pdf_hash(test_doc.pdf_hash)
assert doc.id == test_doc.id

print(" - Try get doc by PDF hash")
doc = store.try_get_doc_by_pdf_hash(test_doc.pdf_hash)
assert doc is not None and doc.id == test_doc.id

print(" - Get doc by non-exist PDF hash")
try:
    store.get_doc_by_pdf_hash("non-exist-pdf-hash")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get doc by non-exist PDF hash")
assert store.try_get_doc_by_pdf_hash("non-exist-pdf-hash") is None

###

print("# Read Pages")

print(" - Find 1 page")
test_page = next(iter(store.find_pages()))

print(" - Find 10 pages")
test_pages = list(store.find_pages(limit=10))
assert len(test_pages) == 10

###

print(" - Get page by ID")
page = store.get_page(test_page.id)
assert page.image_path == test_page.image_path

print(" - Try get page by ID")
page = store.try_get_page(test_page.id)
assert page is not None and page.image_path == test_page.image_path

print(" - Get page by non-exist ID")
try:
    store.get_page("non-exist-page-id")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get page by non-exist ID")
assert store.try_get_page("non-exist-page-id") is None

###

print(" - Get page by image path")
page = store.get_page_by_image_path(test_page.image_path)
assert page.id == test_page.id

print(" - Try get page by image path")
page = store.try_get_page_by_image_path(test_page.image_path)
assert page is not None and page.id == test_page.id

print(" - Get page by non-exist image path")
try:
    store.get_page_by_image_path("non/exist/image/path.png")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get page by non-exist image path")
assert store.try_get_page_by_image_path("non/exist/image/path.png") is None

###

print("# Read Layouts")

print(" - Find 1 layout")
test_layout = next(iter(store.find_layouts()))

print(" - Find 10 layouts")
test_layouts = list(store.find_layouts(limit=10))
assert len(test_layouts) == 10

###

print(" - Get layout by ID")
layout = store.get_layout(test_layout.id)
assert layout.provider == test_layout.provider

print(" - Try get layout by ID")
layout = store.try_get_layout(test_layout.id)
assert layout is not None and layout.provider == test_layout.provider

print(" - Get layout by non-exist ID")
try:
    store.get_layout("non-exist-layout-id")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get layout by non-exist ID")
assert store.try_get_layout("non-exist-layout-id") is None

###

print(" - Get layout by provider")
layout = store.get_layout_by_page_id_and_provider(test_layout.page_id, test_layout.provider)
assert layout.id == test_layout.id

print(" - Try get layout by provider")
layout = store.try_get_layout_by_page_id_and_provider(test_layout.page_id, test_layout.provider)
assert layout is not None and layout.id == test_layout.id

print(" - Get layout by page ID and non-exist provider")
try:
    store.get_layout_by_page_id_and_provider(test_layout.page_id, "non-exist-provider")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get layout by page ID and non-exist provider")
assert store.try_get_layout_by_page_id_and_provider(test_layout.page_id, "non-exist-provider") is None

print(" - Get layout by non-exist page ID and provider")
try:
    store.get_layout_by_page_id_and_provider("non-exist-page-id", test_layout.provider)
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get layout by non-exist page ID and provider")
assert store.try_get_layout_by_page_id_and_provider("non-exist-page-id", test_layout.provider) is None

###

print("# Read Blocks")

print(" - Find 1 block")
test_block = next(iter(store.find_blocks()))

print(" - Find 10 blocks")
test_blocks = list(store.find_blocks(limit=10))
assert len(test_blocks) == 10

###

print(" - Get block by ID")
block = store.get_block(test_block.id)
assert block.bbox == test_block.bbox

print(" - Try get block by ID")
block = store.try_get_block(test_block.id)
assert block is not None and block.bbox == test_block.bbox

print(" - Get block by non-exist ID")
try:
    store.get_block("non-exist-block-id")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get block by non-exist ID")
assert store.try_get_block("non-exist-block-id") is None

###

print(" - Get super block")
super_block = store.get_super_block(test_page.id)
assert super_block.page_id == test_page.id and super_block.type == "super"

print(" - Get super block by non-exist page ID")
try:
    store.get_super_block("non-exist-page-id")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print("# Read Contents")

print(" - Find 1 content")
test_content = next(iter(store.find_contents()))

print(" - Find 10 contents")
test_contents = list(store.find_contents(limit=10))
assert len(test_contents) == 10

###

print(" - Get content by ID")
content = store.get_content(test_content.id)
assert content.block_id == test_content.block_id

print(" - Try get content by ID")
content = store.try_get_content(test_content.id)
assert content is not None and content.block_id == test_content.block_id

print(" - Get content by non-exist ID")
try:
    store.get_content("non-exist-content-id")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get content by non-exist ID")
assert store.try_get_content("non-exist-content-id") is None

###

print(" - Get content by block ID and version")
content = store.get_content_by_block_id_and_version(test_content.block_id, test_content.version)
assert content.id == test_content.id

print(" - Try get content by block ID and version")
content = store.try_get_content_by_block_id_and_version(test_content.block_id, test_content.version)
assert content is not None and content.id == test_content.id

print(" - Get content by block ID and non-exist version")
try:
    store.get_content_by_block_id_and_version(test_content.block_id, "non-exist-version")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get content by block ID and non-exist version")
assert store.try_get_content_by_block_id_and_version(test_content.block_id, "non-exist-version") is None

print(" - Get content by non-exist block ID and version")
try:
    store.get_content_by_block_id_and_version("non-exist-block-id", test_content.version)
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get content by non-exist block ID and version")
assert store.try_get_content_by_block_id_and_version("non-exist-block-id", test_content.version) is None

###

print("# Read Tasks")

print(" - Find 1 task")
test_task = next(iter(store.find_tasks()))

print(" - Find 10 tasks")
test_tasks = list(store.find_tasks(limit=10))
assert len(test_tasks) == 10

###

print(" - Get task by ID")
task = store.get_task(test_task.id)
assert task.target == test_task.target

print(" - Try get task by ID")
task = store.try_get_task(test_task.id)
assert task is not None and task.target == test_task.target

print(" - Get task by non-exist ID")
try:
    store.get_task("non-exist-task-id")
except Exception as e:
    print(f"    - ex: {type(e)} - {e}")
    assert isinstance(e, ElementNotFoundError)

print(" - Try get task by non-exist ID")
assert store.try_get_task("non-exist-task-id") is None

###

print("# Tags")

print(" - List doc tags")
doc_tags = store.doc_tags()
print(f"    - num of doc tags: {len(doc_tags)}")

print(" - List page tags")
page_tags = store.page_tags()
print(f"    - num of page tags: {len(page_tags)}")

print(" - List layout tags")
layout_tags = store.layout_tags()
print(f"    - num of layout tags: {len(layout_tags)}")

print(" - List block tags")
block_tags = store.block_tags()
print(f"    - num of block tags: {len(block_tags)}")

print(" - List content tags")
content_tags = store.content_tags()
print(f"    - num of content tags: {len(content_tags)}")

print(" - List layout providers")
layout_providers = store.layout_providers()
print(f"    - num of layout providers: {len(layout_providers)}")

print(" - List content versions")
content_versions = store.content_versions()
print(f"    - num of content versions: {len(content_versions)}")

print("# Count")

print(" - Count Docs")
docs_count = store.count("doc", estimated=True)
print(f"    - Estimated num of docs: {docs_count}")

print(" - Count Pages")
pages_count = store.count("page", estimated=True)
print(f"    - Estimated num of pages: {pages_count}")

print(" - Count Layouts")
layouts_count = store.count("layout", estimated=True)
print(f"    - Estimated num of layouts: {layouts_count}")

print(" - Count Blocks")
blocks_count = store.count("block", estimated=True)
print(f"    - Estimated num of blocks: {blocks_count}")

print(" - Count Contents")
contents_count = store.count("content", estimated=True)
print(f"    - Estimated num of contents: {contents_count}")

print(" - Count Values")
values_count = store.count("value", estimated=True)
print(f"    - Estimated num of values: {values_count}")

print(" - Count Tasks")
tasks_count = store.count("task", estimated=True)
print(f"    - Estimated num of tasks: {tasks_count}")

###

print("# Add Tag")
test_doc.add_tag("tmp__test_tag")
test_doc = store.get_doc(test_doc.id)
assert "tmp__test_tag" in test_doc.tags

###

print("# Del Tag")
test_doc.del_tag("tmp__test_tag")
test_doc = store.get_doc(test_doc.id)
assert "tmp__test_tag" not in test_doc.tags

###

print("# Add Metric")
test_doc.add_metric("tmp__test_metric", MetricInput(value=123.456))
test_doc = store.get_doc(test_doc.id)
assert "tmp__test_metric" in test_doc.metrics
assert test_doc.metrics["tmp__test_metric"] == 123.456

###

print("# Del Metric")
test_doc.del_metric("tmp__test_metric")
test_doc = store.get_doc(test_doc.id)
assert "tmp__test_metric" not in test_doc.metrics

###

print("# Insert Value")
try:
    test_doc.insert_value("tmp__test_value", ValueInput(value="some-value"))
except ElementExistsError:
    pass
test_value = test_doc.get_value("tmp__test_value")
assert test_value.value == "some-value"

###

print("# Insert Task")
test_doc.insert_task(TaskInput(command="tmp__command", args={"key1": "value1"}))
assert any(task.command == "tmp__command" for task in test_doc.find_tasks())

###

print("# Insert Doc")

###

print("# Insert Page")

###

print("# Insert Layout")
layout = test_page.insert_layout(
    "tmp__test_provider",
    LayoutInput(
        blocks=[
            BlockInput(type="text", bbox=[0, 0, 0.9, 0.1]),
            BlockInput(type="text", bbox=[0, 0.1, 0.9, 0.2]),
            BlockInput(type="text", bbox=[0, 0.2, 0.9, 0.3]),
            BlockInput(type="text", bbox=[0, 0.3, 0.9, 0.4]),
        ],
    ),
    upsert=True,
)
print(layout)

###

print("# Insert Block")
block = test_page.insert_block(BlockInput(type="text", bbox=[0, 0, 0.9, 0.1]))
print(block)

###

print("# Insert Blocks")
blocks = test_page.insert_blocks(
    [
        BlockInput(type="text", bbox=[0, 0, 0.9, 0.1]),
        BlockInput(type="text", bbox=[0, 0.1, 0.9, 0.2]),
        BlockInput(type="text", bbox=[0, 0.2, 0.9, 0.3]),
        BlockInput(type="text", bbox=[0, 0.3, 0.9, 0.4]),
    ]
)
print(blocks)

###

print("# Insert Content")
content = block.insert_content(
    "tmp__test_content",
    ContentInput(format="text", content="This is a test content."),
    upsert=True,
)
print(content)

###

print("# Insert content_blocks_layout")
layout = test_page.insert_content_blocks_layout(
    "tmp__test_provider",
    [
        ContentBlockInput(type="text", bbox=[0, 0, 0.9, 0.1], content="This is a test content 1."),
        ContentBlockInput(type="text", bbox=[0, 0.1, 0.9, 0.2], content="This is a test content 2."),
        ContentBlockInput(type="text", bbox=[0, 0.2, 0.9, 0.3], content="This is a test content 3."),
        ContentBlockInput(type="text", bbox=[0, 0.3, 0.9, 0.4], content="This is a test content 4."),
    ],
    upsert=True,
)
print(layout)

###

print("# Grab Tasks")
tasks = store.grab_new_tasks("tmp__command", num=2)
print(tasks)

###

print("# Update Task")

for task in tasks:
    store.update_grabbed_task(task, "done")
    print(f" - Updated task {task.id} to done")
