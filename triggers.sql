-- update_bmw_preferences_updated_at

BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
