def test_demo_page_explains_the_flow(client):
    response = client.get("/")
    assert response.status_code == 200
    page = response.get_data(as_text=True)
    assert "Save the same event again" in page
    assert "Run the counter" in page
